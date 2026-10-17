// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;

use crate::client::ObjectClient;
use crate::client::ObjectError;
use crate::client::join_key;

/// Which missing trees a restore may fill.
pub struct RestoreSet {
    /// `immutable/index/` has no index files.
    pub index: bool,
    /// `mutable/` has no files.
    pub mutable: bool,
}

/// Failure while copying the index snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The object client failed.
    #[error("{0}")]
    Object(String),
    /// A local file could not be read or written.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

impl From<ObjectError> for SnapshotError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err.to_string())
    }
}

/// Files the snapshot never copies and a restore never counts: the store's
/// process `lock`, the local store's `*.tmp` files (written and then renamed
/// over the real file, so one seen mid-flush is half written), and a restore's
/// own `*.restore-tmp`.
fn is_skipped_file(name: &str) -> bool {
    name == "lock" || name.ends_with(".tmp") || name.ends_with(".restore-tmp")
}

/// True when `dir` contains a file that is not inside a `pack` directory and
/// is not the store's `lock` file or a temporary file.
pub fn has_kept_file(dir: &Path) -> std::io::Result<bool> {
    if !dir.exists() {
        return Ok(false);
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if entry.file_name() == "pack" {
                    continue;
                }
                stack.push(path);
            } else if path.is_file() && !entry.file_name().to_str().is_some_and(is_skipped_file) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Upload `immutable/` and `mutable/`, then write the manifest.
///
/// The local store keeps its index at `immutable/index/` and its branch files
/// under `mutable/`. Pack directories are the cache. The `lock` file in each
/// store directory is held for the life of the process; on Windows that lock
/// is mandatory, so reading it fails with a lock violation. Neither belongs
/// in the snapshot. Older generations are removed after the manifest is in place.
#[cfg(test)]
pub async fn upload(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
) -> Result<(), SnapshotError> {
    upload_if_changed(client, prefix, local, &mut None)
        .await
        .map(|_| ())
}

/// Names, sizes and modification times of the files one snapshot copied.
pub type Fingerprint = Vec<(String, u64, Option<SystemTime>)>;

/// [`upload`], skipped when the files match `last`, the fingerprint of the
/// previous successful upload from this process. Returns whether it uploaded.
///
/// Every snapshot copies the whole index, which is on the order of 3 GiB for
/// a 2 TiB history. An idle guild on the default 60 second timer would send
/// that every minute. A guild with no writes since the last snapshot now
/// sends nothing. `last` is only replaced after the manifest is written.
pub async fn upload_if_changed(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
    last: &mut Option<Fingerprint>,
) -> Result<bool, SnapshotError> {
    let mut files = collect_tree(&local.join("immutable"), "immutable")?;
    files.extend(collect_tree(&local.join("mutable"), "mutable")?);
    let fingerprint = fingerprint(&files);
    if last.as_ref() == Some(&fingerprint) {
        return Ok(false);
    }
    let generation = new_generation();
    let root = format!("snapshot/g/{generation}");
    for (relative, path) in files {
        // A file listed a moment ago can be gone: the local store removes
        // index files when its fan-out changes. It is not part of the index
        // any more, so the snapshot leaves it out.
        let bytes = match read_file(&path) {
            Ok(bytes) => bytes,
            Err(SnapshotError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        client
            .put(&join_key(prefix, &format!("{root}/{relative}")), bytes)
            .await?;
    }
    client
        .put(
            &join_key(prefix, "snapshot/manifest"),
            Bytes::from(generation.clone().into_bytes()),
        )
        .await?;
    if let Err(err) = delete_other_generations(client, prefix, &generation).await {
        tracing::warn!("bucket snapshot left an older generation in place: {err}");
    }
    *last = Some(fingerprint);
    Ok(true)
}

fn fingerprint(files: &[(String, PathBuf)]) -> Fingerprint {
    files
        .iter()
        .map(|(relative, path)| {
            let meta = std::fs::metadata(path).ok();
            (
                relative.clone(),
                meta.as_ref().map_or(0, std::fs::Metadata::len),
                meta.and_then(|meta| meta.modified().ok()),
            )
        })
        .collect()
}

/// Fill the trees named in `which` from the manifest's generation.
///
/// No manifest means there is nothing to restore.
pub async fn restore(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
    which: RestoreSet,
) -> Result<(), SnapshotError> {
    let manifest = join_key(prefix, "snapshot/manifest");
    let generation = match client.get(&manifest).await {
        Ok(bytes) => String::from_utf8(bytes.to_vec()).map_err(|err| {
            SnapshotError::Object(format!("snapshot manifest is not text: {err}"))
        })?,
        Err(ObjectError::NotFound { .. }) => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    let generation = generation.trim();
    if generation.is_empty() || generation.contains('/') {
        return Err(SnapshotError::Object(
            "snapshot manifest names an empty generation".to_string(),
        ));
    }
    let list_prefix = join_key(prefix, &format!("snapshot/g/{generation}/"));
    let keys = client.list(&list_prefix).await?;
    for key in keys {
        let Some(relative) = key.strip_prefix(&list_prefix) else {
            continue;
        };
        let keep = (which.index && relative.starts_with("immutable/"))
            || (which.mutable && relative.starts_with("mutable/"));
        if !keep {
            continue;
        }
        let Some(path) = local_file(local, relative) else {
            continue;
        };
        let bytes = client.get(&key).await?;
        write_file(&path, &bytes)?;
    }
    Ok(())
}

fn new_generation() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    format!("{ms}-{n}")
}

fn collect_tree(dir: &Path, key_prefix: &str) -> std::io::Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let mut stack = vec![(dir.to_path_buf(), key_prefix.to_string())];
    while let Some((current, prefix)) = stack.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                if name == "pack" {
                    continue;
                }
                stack.push((path, format!("{prefix}/{name}")));
            } else if path.is_file() && !is_skipped_file(name) {
                out.push((format!("{prefix}/{name}"), path));
            }
        }
    }
    out.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(out)
}

async fn delete_other_generations(
    client: &dyn ObjectClient,
    prefix: &str,
    current: &str,
) -> Result<(), SnapshotError> {
    let list_prefix = join_key(prefix, "snapshot/g/");
    let keys = client.list(&list_prefix).await?;
    for key in keys {
        let Some(rest) = key.strip_prefix(&list_prefix) else {
            continue;
        };
        let Some(generation) = rest.split('/').next() else {
            continue;
        };
        if generation != current {
            client.delete(&key).await?;
        }
    }
    Ok(())
}

fn local_file(root: &Path, relative: &str) -> Option<PathBuf> {
    let mut path = root.to_path_buf();
    for part in relative.split('/') {
        if part.is_empty() || part == "." || part == ".." || part == "pack" || part == "lock" {
            return None;
        }
        path.push(part);
    }
    Some(path)
}

fn read_file(path: &Path) -> Result<Bytes, SnapshotError> {
    std::fs::read(path).map(Bytes::from).map_err(|err| {
        SnapshotError::Io(std::io::Error::new(
            err.kind(),
            format!("reading {}: {err}", path.display()),
        ))
    })
}

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(".restore-tmp");
    let temporary = PathBuf::from(temporary);
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ObjectClient;
    use crate::memory::MemoryClient;

    #[tokio::test]
    async fn snapshot_skips_packs_and_restores_a_missing_index() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(index.join("pack")).unwrap();
        std::fs::write(index.join("index_00"), b"index-bytes").unwrap();
        std::fs::write(index.join("pack").join("data"), b"pack-bytes").unwrap();
        std::fs::write(root.path().join("immutable").join("lock"), b"held").unwrap();
        let mutable = root.path().join("mutable");
        std::fs::create_dir_all(&mutable).unwrap();
        std::fs::write(mutable.join("branches"), b"branch-bytes").unwrap();
        std::fs::write(mutable.join("lock"), b"held").unwrap();

        upload(client.as_ref(), "guilds/one", root.path())
            .await
            .unwrap();

        let keys = client.list("guilds/one/snapshot/").await.unwrap();
        assert!(
            keys.iter()
                .any(|key| key.ends_with("/immutable/index/aa/index_00"))
        );
        assert!(keys.iter().any(|key| key.ends_with("/mutable/branches")));
        assert!(keys.iter().all(|key| !key.contains("/pack/")));
        assert!(keys.iter().all(|key| !key.ends_with("/lock")));

        std::fs::remove_dir_all(root.path().join("immutable").join("index")).unwrap();
        std::fs::remove_file(mutable.join("branches")).unwrap();
        restore(
            client.as_ref(),
            "guilds/one",
            root.path(),
            RestoreSet {
                index: true,
                mutable: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            std::fs::read(root.path().join("immutable/index/aa/index_00")).unwrap(),
            b"index-bytes"
        );
        assert_eq!(
            std::fs::read(root.path().join("mutable/branches")).unwrap(),
            b"branch-bytes"
        );
    }

    #[tokio::test]
    async fn restore_leaves_a_present_index_in_place() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"first").unwrap();
        upload(client.as_ref(), "", root.path()).await.unwrap();

        std::fs::write(index.join("index_00"), b"live").unwrap();
        restore(
            client.as_ref(),
            "",
            root.path(),
            RestoreSet {
                index: false,
                mutable: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(index.join("index_00")).unwrap(), b"live");

        std::fs::remove_dir_all(root.path().join("immutable").join("index")).unwrap();
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"still-live").unwrap();
        restore(
            client.as_ref(),
            "",
            root.path(),
            RestoreSet {
                index: false,
                mutable: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(index.join("index_00")).unwrap(),
            b"still-live"
        );
    }

    #[tokio::test]
    async fn snapshot_reads_while_the_store_holds_its_lock() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let immutable = root.path().join("immutable");
        let index = immutable.join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"index-bytes").unwrap();
        let mutable = root.path().join("mutable");
        std::fs::create_dir_all(&mutable).unwrap();
        std::fs::write(mutable.join("branches"), b"branch-bytes").unwrap();
        let _immutable_lock = lore_base::fs::lock::FSLock::acquire_directory_lock(&immutable)
            .await
            .unwrap();
        let _mutable_lock = lore_base::fs::lock::FSLock::acquire_directory_lock(&mutable)
            .await
            .unwrap();

        upload(client.as_ref(), "", root.path()).await.unwrap();

        let keys = client.list("snapshot/").await.unwrap();
        assert!(
            keys.iter()
                .any(|key| key.ends_with("/immutable/index/aa/index_00"))
        );
        assert!(keys.iter().any(|key| key.ends_with("/mutable/branches")));
        assert!(keys.iter().all(|key| !key.ends_with("/lock")));
    }

    #[tokio::test]
    async fn snapshot_skips_temporary_files_and_unchanged_trees() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"index-bytes").unwrap();
        std::fs::write(index.join("index_01.tmp"), b"half").unwrap();

        let mut last = None;
        assert!(
            upload_if_changed(client.as_ref(), "", root.path(), &mut last)
                .await
                .unwrap()
        );
        let keys = client.list("snapshot/g/").await.unwrap();
        assert!(keys.iter().any(|key| key.ends_with("/index_00")));
        assert!(keys.iter().all(|key| !key.ends_with(".tmp")));

        // Nothing changed: no new generation.
        assert!(
            !upload_if_changed(client.as_ref(), "", root.path(), &mut last)
                .await
                .unwrap()
        );
        assert_eq!(client.list("snapshot/g/").await.unwrap(), keys);

        // A changed file uploads again.
        std::fs::write(index.join("index_00"), b"index-bytes-2").unwrap();
        assert!(
            upload_if_changed(client.as_ref(), "", root.path(), &mut last)
                .await
                .unwrap()
        );
        assert_ne!(client.list("snapshot/g/").await.unwrap(), keys);
    }

    #[test]
    fn a_lone_temporary_file_is_not_an_index() {
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00.tmp"), b"half").unwrap();
        assert!(!has_kept_file(&root.path().join("index")).unwrap());
    }
}
