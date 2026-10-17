// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use serde::Deserialize;

use crate::BucketError;

fn default_snapshot_interval() -> u64 {
    60
}

/// `[plugins.bucket]` table.
#[derive(Debug, Clone, Deserialize)]
pub struct BucketConfig {
    /// Bucket name.
    pub bucket: String,
    /// Region string the SDK requires. Railway fixes this at bucket creation.
    pub region: String,
    /// S3-compatible endpoint. Absent uses the default AWS endpoint.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Path-style addressing. Virtual-hosted style is the default.
    #[serde(default)]
    pub path_style: bool,
    /// Key prefix inside a shared bucket. Empty means the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Static access key. Set together with `secret_access_key`, or leave both
    /// unset and use the default credential chain.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Static secret for `access_key_id`.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Directory that holds `index/` and `mutable/`. `loreserver` fills this
    /// from the composite local store path.
    #[serde(default)]
    pub local_path: Option<String>,
    /// Seconds between index snapshots. Zero leaves snapshots to flush only.
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_seconds: u64,
}

impl BucketConfig {
    /// Parse and check the plugin table.
    pub fn parse(value: &toml::Value) -> Result<Self, BucketError> {
        let cfg: Self = value.clone().try_into().map_err(|err: toml::de::Error| {
            BucketError::Config(format!("failed to read bucket config: {err}"))
        })?;
        cfg.check()?;
        Ok(cfg)
    }

    /// Prefix with surrounding slashes removed.
    pub fn normalized_prefix(&self) -> String {
        self.prefix.trim_matches('/').to_string()
    }

    fn check(&self) -> Result<(), BucketError> {
        if self.bucket.is_empty() {
            return Err(BucketError::Config("bucket name is empty".to_string()));
        }
        if self.region.is_empty() {
            return Err(BucketError::Config("region is empty".to_string()));
        }
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(_), None) | (None, Some(_)) => Err(BucketError::Config(
                "access_key_id and secret_access_key must both be set, or both left unset"
                    .to_string(),
            )),
            _ => Ok(()),
        }
    }
}
