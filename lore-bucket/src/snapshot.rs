// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use futures::stream::StreamExt;
use lore_storage::local::immutable_store::index_content_hash;
use lore_storage::local::immutable_store::snapshot_blake3;

use crate::client::ObjectClient;
use crate::client::ObjectError;
use crate::client::join_key;

/// How many object uploads in one batch run at once.
const UPLOAD_PARALLELISM: usize = 32;
/// Walks that see the tree change underneath them are retried, then refused.
const WALK_ATTEMPTS: u32 = 8;

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
    /// The tree changed while it was being read. The batch was not published.
    #[error("snapshot walk overlapped a flush")]
    Unstable,
}

impl From<ObjectError> for SnapshotError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err.to_string())
    }
}

/// One published file. `raw_hash` names the object. `content_hash` ignores
/// access stamps, so a read-only rewrite does not upload the file again.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapFile {
    raw_hash: String,
    content_hash: String,
    size: u64,
    /// `None` until this process has seen the file. A loaded manifest has no mtime.
    mtime: Option<SystemTime>,
    len: u64,
}

/// Files the last successful batch published, and whether the legacy full-copy
/// generation prefix has been removed.
#[derive(Default)]
pub struct BatchState {
    files: BTreeMap<String, SnapFile>,
    generation: Option<String>,
    remote_loaded: bool,
    legacy_cleared: bool,
}

/// Files the snapshot never copies and a restore never counts.
fn is_skipped_file(name: &str) -> bool {
    name == "lock"
        || name == "level.pending"
        || name.ends_with(".tmp")
        || name.ends_with(".new")
        || name.ends_with(".restore-tmp")
}

/// An index bucket file (`immutable/index/<gg>/index_<bb>`), not a level marker.
fn is_index_bucket(relative: &str) -> bool {
    let Some(name) = relative.rsplit('/').next() else {
        return false;
    };
    relative.contains("/index/") && name.starts_with("index_") && !name.contains('.')
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

/// Upload changed index and branch files as one batch, then write the manifest.
#[cfg(test)]
pub async fn upload(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
) -> Result<(), SnapshotError> {
    upload_if_changed(client, prefix, local, &mut BatchState::default())
        .await
        .map(|_| ())
}

/// [`upload`], skipped when nothing that restore needs has changed.
/// Returns whether a new manifest was written. `state` is updated only after
/// that write.
pub async fn upload_if_changed(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
    state: &mut BatchState,
) -> Result<bool, SnapshotError> {
    if !state.remote_loaded {
        load_remote(client, prefix, state).await?;
        state.remote_loaded = true;
    }

    let mut last_unstable = None;
    for _ in 0..WALK_ATTEMPTS {
        match publish_stable(client, prefix, local, state).await {
            Ok(published) => return Ok(published),
            Err(SnapshotError::Unstable) => {
                last_unstable = Some(SnapshotError::Unstable);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_unstable.unwrap_or(SnapshotError::Unstable))
}

async fn publish_stable(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
    state: &mut BatchState,
) -> Result<bool, SnapshotError> {
    let before = fingerprint(local)?;
    let diff = diff_tree(local, &state.files)?;
    let after = fingerprint(local)?;
    if before != after {
        return Err(SnapshotError::Unstable);
    }
    if !diff.saw_file && !state.files.is_empty() {
        tracing::warn!(
            "bucket snapshot left the manifest in place: local index and branch files are absent"
        );
        return Ok(false);
    }
    if !diff.changed {
        state.files = diff.files;
        clear_legacy_if_current(client, prefix, state).await;
        return Ok(false);
    }

    let generation = new_generation();
    let uploads = diff
        .uploads
        .iter()
        .map(|(hash, bytes)| {
            (
                join_key(prefix, &format!("snapshot/obj/{hash}")),
                bytes.clone(),
            )
        })
        .collect::<Vec<_>>();
    futures::stream::iter(uploads)
        .map(|(key, bytes)| async move { client.put(&key, bytes).await })
        .buffer_unordered(UPLOAD_PARALLELISM)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let tree = render_tree(&diff.files);
    client
        .put(
            &join_key(prefix, &format!("snapshot/tree/{generation}")),
            Bytes::from(tree),
        )
        .await?;
    client
        .put(
            &join_key(prefix, "snapshot/manifest"),
            Bytes::from(generation.clone().into_bytes()),
        )
        .await?;

    let previous = state.generation.clone();
    state.files = diff.files;
    state.generation = Some(generation);
    if let Err(err) = gc_objects(client, prefix, &state.files).await {
        tracing::warn!("bucket snapshot left an unreferenced object in place: {err}");
    }
    if let Some(previous) = previous
        && state.generation.as_deref() != Some(previous.as_str())
        && let Err(err) = client
            .delete(&join_key(prefix, &format!("snapshot/tree/{previous}")))
            .await
    {
        tracing::warn!("bucket snapshot left an older tree in place: {err}");
    }
    clear_legacy_if_current(client, prefix, state).await;
    Ok(true)
}

struct Diff {
    files: BTreeMap<String, SnapFile>,
    uploads: Vec<(String, Bytes)>,
    changed: bool,
    saw_file: bool,
}

fn fingerprint(local: &Path) -> Result<Vec<(String, u64, Option<SystemTime>)>, SnapshotError> {
    let mut listed = collect_tree(&local.join("immutable"), "immutable")?;
    listed.extend(collect_tree(&local.join("mutable"), "mutable")?);
    let mut out = Vec::with_capacity(listed.len());
    for (relative, path) in listed {
        let meta = std::fs::metadata(&path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                SnapshotError::Unstable
            } else {
                SnapshotError::Io(err)
            }
        })?;
        out.push((relative, meta.len(), meta.modified().ok()));
    }
    Ok(out)
}

fn diff_tree(local: &Path, previous: &BTreeMap<String, SnapFile>) -> Result<Diff, SnapshotError> {
    let mut listed = collect_tree(&local.join("immutable"), "immutable")?;
    listed.extend(collect_tree(&local.join("mutable"), "mutable")?);
    let saw_file = !listed.is_empty();
    let mut files = BTreeMap::new();
    let mut uploads = Vec::new();
    let mut changed = false;

    for (relative, path) in listed {
        let meta = match std::fs::metadata(&path) {
            Ok(meta) => Some(meta),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(SnapshotError::Unstable);
            }
            Err(_) => None,
        };
        let len = meta.as_ref().map_or(0, std::fs::Metadata::len);
        let mtime = meta.and_then(|meta| meta.modified().ok());
        if let Some(prev) = previous.get(&relative)
            && prev.mtime.is_some()
            && prev.mtime == mtime
            && prev.len == len
        {
            files.insert(relative, prev.clone());
            continue;
        }
        let bytes = match read_file(&path) {
            Ok(bytes) => bytes,
            Err(SnapshotError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(SnapshotError::Unstable);
            }
            Err(err) => return Err(err),
        };
        let raw_hash = hex::encode(snapshot_blake3(&bytes));
        let content_hash = if is_index_bucket(&relative) {
            hex::encode(index_content_hash(&bytes))
        } else {
            raw_hash.clone()
        };
        if let Some(prev) = previous.get(&relative)
            && (prev.content_hash == content_hash || prev.raw_hash == raw_hash)
        {
            files.insert(
                relative,
                SnapFile {
                    raw_hash: prev.raw_hash.clone(),
                    content_hash,
                    size: prev.size,
                    mtime,
                    len,
                },
            );
            continue;
        }
        changed = true;
        let size = bytes.len() as u64;
        uploads.push((raw_hash.clone(), bytes));
        files.insert(
            relative,
            SnapFile {
                raw_hash,
                content_hash,
                size,
                mtime,
                len,
            },
        );
    }
    if files.len() != previous.len() {
        changed = true;
    }
    Ok(Diff {
        files,
        uploads,
        changed,
        saw_file,
    })
}

fn render_tree(files: &BTreeMap<String, SnapFile>) -> Vec<u8> {
    let mut out = Vec::new();
    for (path, file) in files {
        out.extend(
            format!(
                "{path} {} {} {}\n",
                file.raw_hash, file.content_hash, file.size
            )
            .into_bytes(),
        );
    }
    out
}

fn parse_tree(bytes: &[u8]) -> Result<BTreeMap<String, SnapFile>, SnapshotError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| SnapshotError::Object(format!("snapshot tree is not text: {err}")))?;
    let mut files = BTreeMap::new();
    for (line_number, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split(' ');
        let (Some(path), Some(raw_hash), Some(content_hash), Some(size), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(SnapshotError::Object(format!(
                "snapshot tree line {} is malformed",
                line_number + 1
            )));
        };
        let size = size.parse::<u64>().map_err(|err| {
            SnapshotError::Object(format!(
                "snapshot tree line {} has a bad size: {err}",
                line_number + 1
            ))
        })?;
        files.insert(
            path.to_string(),
            SnapFile {
                raw_hash: raw_hash.to_string(),
                content_hash: content_hash.to_string(),
                size,
                mtime: None,
                len: 0,
            },
        );
    }
    Ok(files)
}

async fn load_remote(
    client: &dyn ObjectClient,
    prefix: &str,
    state: &mut BatchState,
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
    if !generation.starts_with("v2-") {
        return Ok(());
    }
    let tree = client
        .get(&join_key(prefix, &format!("snapshot/tree/{generation}")))
        .await?;
    state.files = parse_tree(&tree)?;
    state.generation = Some(generation.to_string());
    Ok(())
}

async fn gc_objects(
    client: &dyn ObjectClient,
    prefix: &str,
    files: &BTreeMap<String, SnapFile>,
) -> Result<(), SnapshotError> {
    let keep = files
        .values()
        .map(|file| file.raw_hash.as_str())
        .collect::<HashSet<_>>();
    let list_prefix = join_key(prefix, "snapshot/obj/");
    for key in client.list(&list_prefix).await? {
        let Some(hash) = key.strip_prefix(&list_prefix) else {
            continue;
        };
        if !keep.contains(hash) {
            client.delete(&key).await?;
        }
    }
    Ok(())
}

async fn clear_legacy_if_current(client: &dyn ObjectClient, prefix: &str, state: &mut BatchState) {
    if state.legacy_cleared
        || !state
            .generation
            .as_ref()
            .is_some_and(|id| id.starts_with("v2-"))
    {
        return;
    }
    let list_prefix = join_key(prefix, "snapshot/g/");
    match client.list(&list_prefix).await {
        Ok(keys) => {
            for key in keys {
                if let Err(err) = client.delete(&key).await {
                    tracing::warn!("bucket snapshot left a legacy generation in place: {err}");
                    return;
                }
            }
            state.legacy_cleared = true;
        }
        Err(err) => tracing::warn!("bucket snapshot could not list legacy generations: {err}"),
    }
}

/// Fill the trees named in `which` from the manifest.
///
/// A manifest whose id starts with `v2-` is a tree of content-addressed
/// objects. Any other id is the older full-copy layout under `snapshot/g/`.
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
    if generation.starts_with("v2-") {
        restore_tree(client, prefix, local, generation, &which).await
    } else {
        restore_legacy(client, prefix, local, generation, &which).await
    }
}

async fn restore_tree(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
    generation: &str,
    which: &RestoreSet,
) -> Result<(), SnapshotError> {
    let tree = client
        .get(&join_key(prefix, &format!("snapshot/tree/{generation}")))
        .await?;
    for (relative, file) in parse_tree(&tree)? {
        if !restore_keeps(&relative, which) {
            continue;
        }
        let Some(path) = local_file(local, &relative) else {
            continue;
        };
        let bytes = client
            .get(&join_key(
                prefix,
                &format!("snapshot/obj/{}", file.raw_hash),
            ))
            .await?;
        let got = hex::encode(snapshot_blake3(&bytes));
        if got != file.raw_hash || bytes.len() as u64 != file.size {
            return Err(SnapshotError::Object(format!(
                "snapshot object {relative} failed its hash check"
            )));
        }
        write_file(&path, &bytes)?;
    }
    Ok(())
}

async fn restore_legacy(
    client: &dyn ObjectClient,
    prefix: &str,
    local: &Path,
    generation: &str,
    which: &RestoreSet,
) -> Result<(), SnapshotError> {
    let list_prefix = join_key(prefix, &format!("snapshot/g/{generation}/"));
    let keys = client.list(&list_prefix).await?;
    for key in keys {
        let Some(relative) = key.strip_prefix(&list_prefix) else {
            continue;
        };
        if !restore_keeps(relative, which) {
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

fn restore_keeps(relative: &str, which: &RestoreSet) -> bool {
    (which.index && relative.starts_with("immutable/"))
        || (which.mutable && relative.starts_with("mutable/"))
}

fn new_generation() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    format!("v2-{ms}-{n}")
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
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use zerocopy::IntoBytes;

    async fn tree_text(client: &MemoryClient, prefix: &str) -> String {
        let manifest = client
            .get(&join_key(prefix, "snapshot/manifest"))
            .await
            .unwrap();
        let generation = String::from_utf8(manifest.to_vec()).unwrap();
        let tree = client
            .get(&join_key(
                prefix,
                &format!("snapshot/tree/{}", generation.trim()),
            ))
            .await
            .unwrap();
        String::from_utf8(tree.to_vec()).unwrap()
    }

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

        let text = tree_text(&client, "guilds/one").await;
        assert!(text.contains("immutable/index/aa/index_00"));
        assert!(text.contains("mutable/branches"));
        assert!(!text.contains("pack"));
        assert!(!text.contains("lock"));

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

        let text = tree_text(&client, "").await;
        assert!(text.contains("immutable/index/aa/index_00"));
        assert!(text.contains("mutable/branches"));
        assert!(!text.contains("lock"));
    }

    #[tokio::test]
    async fn snapshot_skips_temporary_files_and_unchanged_trees() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"index-bytes").unwrap();
        std::fs::write(index.join("index_01.tmp"), b"half").unwrap();
        std::fs::write(index.join("index_02.new"), b"fanout").unwrap();
        std::fs::write(index.join("level.pending"), b"pending").unwrap();

        let mut state = BatchState::default();
        assert!(
            upload_if_changed(client.as_ref(), "", root.path(), &mut state)
                .await
                .unwrap()
        );
        let manifest = client.get("snapshot/manifest").await.unwrap();
        let text = tree_text(&client, "").await;
        assert!(text.contains("index_00"));
        assert!(!text.contains(".tmp"));
        assert!(!text.contains(".new"));
        assert!(!text.contains("level.pending"));

        assert!(
            !upload_if_changed(client.as_ref(), "", root.path(), &mut state)
                .await
                .unwrap()
        );
        assert_eq!(client.get("snapshot/manifest").await.unwrap(), manifest);

        std::fs::write(index.join("index_00"), b"index-bytes-2").unwrap();
        assert!(
            upload_if_changed(client.as_ref(), "", root.path(), &mut state)
                .await
                .unwrap()
        );
        assert_ne!(client.get("snapshot/manifest").await.unwrap(), manifest);
        let restored = tempfile::tempdir().unwrap();
        restore(
            client.as_ref(),
            "",
            restored.path(),
            RestoreSet {
                index: true,
                mutable: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(restored.path().join("immutable/index/aa/index_00")).unwrap(),
            b"index-bytes-2"
        );
    }

    #[tokio::test]
    async fn a_batch_uploads_every_changed_file_before_the_manifest() {
        let client = OrderClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"one").unwrap();
        std::fs::write(index.join("index_01"), b"two").unwrap();
        let mutable = root.path().join("mutable");
        std::fs::create_dir_all(&mutable).unwrap();
        std::fs::write(mutable.join("branches"), b"tip").unwrap();

        upload(&client, "", root.path()).await.unwrap();

        let order = client.order.lock().unwrap().clone();
        let manifest = order
            .iter()
            .rposition(|key| key == "snapshot/manifest")
            .unwrap();
        let tree = order
            .iter()
            .rposition(|key| key.starts_with("snapshot/tree/"))
            .unwrap();
        assert!(tree < manifest);
        assert_eq!(
            order
                .iter()
                .filter(|key| key.starts_with("snapshot/obj/"))
                .count(),
            3
        );
        assert!(
            order
                .iter()
                .take(manifest)
                .filter(|key| key.starts_with("snapshot/obj/"))
                .count()
                >= 3
        );
    }

    #[tokio::test]
    async fn an_access_stamp_rewrite_is_not_uploaded_again() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        let path = index.join("index_00");
        std::fs::write(&path, index_bytes(1)).unwrap();

        let mut state = BatchState::default();
        assert!(
            upload_if_changed(client.as_ref(), "", root.path(), &mut state)
                .await
                .unwrap()
        );
        let objects = client.list("snapshot/obj/").await.unwrap();
        std::fs::write(&path, index_bytes(2)).unwrap();
        assert!(
            !upload_if_changed(client.as_ref(), "", root.path(), &mut state)
                .await
                .unwrap()
        );
        assert_eq!(client.list("snapshot/obj/").await.unwrap(), objects);

        std::fs::write(&path, index_bytes_with_content(2, 9)).unwrap();
        assert!(
            upload_if_changed(client.as_ref(), "", root.path(), &mut state)
                .await
                .unwrap()
        );
        assert_ne!(client.list("snapshot/obj/").await.unwrap(), objects);
    }

    #[tokio::test]
    async fn a_failed_upload_does_not_move_the_manifest() {
        let inner = MemoryClient::new();
        let client = FailPut::new(Arc::clone(&inner));
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"first").unwrap();
        let mut state = BatchState::default();
        upload_if_changed(&client, "", root.path(), &mut state)
            .await
            .unwrap();
        let manifest = inner.get("snapshot/manifest").await.unwrap();

        client.fail.store(true, Ordering::Relaxed);
        std::fs::write(index.join("index_00"), b"second").unwrap();
        assert!(
            upload_if_changed(&client, "", root.path(), &mut state)
                .await
                .is_err()
        );
        assert_eq!(inner.get("snapshot/manifest").await.unwrap(), manifest);

        client.fail.store(false, Ordering::Relaxed);
        assert!(
            upload_if_changed(&client, "", root.path(), &mut state)
                .await
                .unwrap()
        );
        let fresh = tempfile::tempdir().unwrap();
        restore(
            inner.as_ref(),
            "",
            fresh.path(),
            RestoreSet {
                index: true,
                mutable: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(fresh.path().join("immutable/index/aa/index_00")).unwrap(),
            b"second"
        );
    }

    #[tokio::test]
    async fn restore_still_reads_a_legacy_generation() {
        let client = MemoryClient::new();
        client
            .put(
                "snapshot/g/old/immutable/index/aa/index_00",
                Bytes::from_static(b"legacy-index"),
            )
            .await
            .unwrap();
        client
            .put(
                "snapshot/g/old/mutable/branches",
                Bytes::from_static(b"legacy-tip"),
            )
            .await
            .unwrap();
        client
            .put("snapshot/manifest", Bytes::from_static(b"old"))
            .await
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        restore(
            client.as_ref(),
            "",
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
            b"legacy-index"
        );
        assert_eq!(
            std::fs::read(root.path().join("mutable/branches")).unwrap(),
            b"legacy-tip"
        );
    }

    #[tokio::test]
    async fn restore_rejects_an_object_that_fails_its_hash_check() {
        let client = MemoryClient::new();
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("immutable").join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00"), b"index-bytes").unwrap();
        upload(client.as_ref(), "", root.path()).await.unwrap();
        let text = tree_text(&client, "").await;
        let hash = text.split_whitespace().nth(1).unwrap();
        client
            .put(
                &format!("snapshot/obj/{hash}"),
                Bytes::from_static(b"tampered"),
            )
            .await
            .unwrap();
        std::fs::remove_dir_all(root.path().join("immutable").join("index")).unwrap();
        let err = restore(
            client.as_ref(),
            "",
            root.path(),
            RestoreSet {
                index: true,
                mutable: false,
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("hash check"), "{err}");
    }

    #[test]
    fn a_lone_temporary_file_is_not_an_index() {
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("index").join("aa");
        std::fs::create_dir_all(&index).unwrap();
        std::fs::write(index.join("index_00.tmp"), b"half").unwrap();
        assert!(!has_kept_file(&root.path().join("index")).unwrap());
    }

    fn index_bytes(stamp: u64) -> Vec<u8> {
        index_bytes_with_content(stamp, 7)
    }

    fn index_bytes_with_content(stamp: u64, size_content: u64) -> Vec<u8> {
        use lore_storage::local::immutable_store::ImmutableStoreEntry;
        let mut entry = ImmutableStoreEntry::default();
        entry.data.last_access = stamp;
        entry.data.size_content = size_content;
        let mut out = vec![0u8; 16];
        out[0] = 5;
        out[8] = 1;
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(entry.as_bytes());
        out
    }

    struct OrderClient {
        inner: Arc<MemoryClient>,
        order: Mutex<Vec<String>>,
    }

    impl OrderClient {
        fn new() -> Self {
            Self {
                inner: MemoryClient::new(),
                order: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectClient for OrderClient {
        async fn put(&self, key: &str, body: Bytes) -> Result<(), ObjectError> {
            self.order.lock().unwrap().push(key.to_string());
            self.inner.put(key, body).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, ObjectError> {
            self.inner.get(key).await
        }
        async fn head(&self, key: &str) -> Result<bool, ObjectError> {
            self.inner.head(key).await
        }
        async fn delete(&self, key: &str) -> Result<(), ObjectError> {
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
            self.inner.list(prefix).await
        }
    }

    struct FailPut {
        inner: Arc<MemoryClient>,
        fail: AtomicBool,
    }

    impl FailPut {
        fn new(inner: Arc<MemoryClient>) -> Self {
            Self {
                inner,
                fail: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectClient for FailPut {
        async fn put(&self, key: &str, body: Bytes) -> Result<(), ObjectError> {
            if self.fail.load(Ordering::Relaxed) && key.starts_with("snapshot/obj/") {
                return Err(ObjectError::failed("put refused"));
            }
            self.inner.put(key, body).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, ObjectError> {
            self.inner.get(key).await
        }
        async fn head(&self, key: &str) -> Result<bool, ObjectError> {
            self.inner.head(key).await
        }
        async fn delete(&self, key: &str) -> Result<(), ObjectError> {
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
            self.inner.list(prefix).await
        }
    }
}
