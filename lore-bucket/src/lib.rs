// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT
//! Durable Lore tier for an S3-compatible bucket.
//!
//! Payloads and association markers live in the bucket. The local Lore index
//! answers lookups. A snapshot of `index/` (without pack files) and `mutable/`
//! is uploaded after flush so a missing local index can be restored.

mod client;
mod config;
#[cfg(test)]
mod memory;
mod s3;
mod snapshot;
mod store;

pub use config::BucketConfig;
pub use store::BucketStore;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lore_storage::ImmutableStore;

/// Why the bucket tier could not be configured or opened.
#[derive(Debug, thiserror::Error)]
pub enum BucketError {
    /// The TOML table is missing a field or names an empty bucket.
    #[error("{0}")]
    Config(String),
    /// The bucket could not be reached, or a restore failed.
    #[error("{0}")]
    Init(String),
}

/// Restore a missing local index and branch tree, then return.
///
/// A directory that already holds index files, or mutable files, is left
/// alone. Pack files do not count as an index. A bucket with no snapshot
/// manifest is a new guild and is also left alone.
pub async fn prepare(config: &toml::Value) -> Result<(), BucketError> {
    let cfg = BucketConfig::parse(config)?;
    let Some(local) = cfg.local_path.as_ref() else {
        tracing::info!("bucket prepare skipped: no local_path");
        return Ok(());
    };
    let local = PathBuf::from(local);
    let index_absent = !snapshot::has_kept_file(&local.join("index")).map_err(BucketError::io)?;
    let mutable_absent =
        !snapshot::has_kept_file(&local.join("mutable")).map_err(BucketError::io)?;
    if !index_absent && !mutable_absent {
        tracing::info!(path = %local.display(), "bucket prepare: local index is present");
        return Ok(());
    }

    let client = s3::connect(&cfg).await?;
    snapshot::restore(
        client.as_ref(),
        &cfg.normalized_prefix(),
        &local,
        snapshot::RestoreSet {
            index: index_absent,
            mutable: mutable_absent,
        },
    )
    .await
    .map_err(BucketError::io)?;
    Ok(())
}

/// Open the durable store and start the snapshot timer when one is configured.
pub async fn open(config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, BucketError> {
    let cfg = BucketConfig::parse(config)?;
    let client = s3::connect(&cfg).await?;
    let store = Arc::new(BucketStore::new(
        client,
        cfg.normalized_prefix(),
        cfg.local_path.as_ref().map(PathBuf::from),
    ));
    store.start_periodic(Duration::from_secs(cfg.snapshot_interval_seconds));
    Ok(store)
}

impl BucketError {
    fn io(err: impl std::fmt::Display) -> Self {
        Self::Init(err.to_string())
    }
}
