// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use serde::Deserialize;

use crate::BucketError;

fn default_snapshot_interval() -> u64 {
    60
}

/// `[plugins.bucket]` table.
///
/// Dragon does not use Amazon Web Services. `endpoint` is an S3-compatible
/// service. The access key and secret are required in this table.
#[derive(Clone, Deserialize)]
pub struct BucketConfig {
    /// Bucket name.
    pub bucket: String,
    /// Region name that service expects. Railway fixes it when the bucket is
    /// created. The local Silo uses `us-east-1`.
    pub region: String,
    /// Base URL of the S3-compatible service, including the scheme.
    pub endpoint: String,
    /// Path-style URLs. The local Silo needs this. Virtual-hosted style is the default.
    #[serde(default)]
    pub path_style: bool,
    /// Key prefix inside a shared bucket. Empty means the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Access key for `endpoint`.
    pub access_key_id: String,
    /// Secret for `access_key_id`.
    pub secret_access_key: String,
    /// Cache directory: the parent of `immutable/` and `mutable/`.
    ///
    /// `loreserver` sets this from the composite local `path` and replaces a
    /// value written in the table. Absent only when the library is opened
    /// without that server path, in which case no snapshot is taken.
    #[serde(default)]
    pub local_path: Option<String>,
    /// Seconds between index snapshots. Zero snapshots only when the store is
    /// flushed. `loreserver` does that at a clean stop, not while it is running.
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_seconds: u64,
}

/// Prints every field except the secret, so a `{:?}` in a log line or an
/// error message cannot leak it.
impl std::fmt::Debug for BucketConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("path_style", &self.path_style)
            .field("prefix", &self.prefix)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("local_path", &self.local_path)
            .field("snapshot_interval_seconds", &self.snapshot_interval_seconds)
            .finish()
    }
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
        if self.endpoint.is_empty() {
            return Err(BucketError::Config("endpoint is empty".to_string()));
        }
        if self.access_key_id.is_empty() || self.secret_access_key.is_empty() {
            return Err(BucketError::Config(
                "access_key_id and secret_access_key are required".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BucketConfig;

    fn value(text: &str) -> toml::Value {
        toml::from_str(text).expect("test toml")
    }

    #[test]
    fn parse_accepts_the_local_bucket() {
        let cfg = BucketConfig::parse(&value(
            r#"
            bucket = "dragon"
            region = "us-east-1"
            endpoint = "http://127.0.0.1:9000"
            path_style = true
            access_key_id = "dragon"
            secret_access_key = "dragonsecret"
            "#,
        ))
        .expect("config");
        assert_eq!(cfg.endpoint, "http://127.0.0.1:9000");
        assert!(cfg.path_style);
        assert!(cfg.local_path.is_none());
        assert_eq!(cfg.snapshot_interval_seconds, 60);
    }

    #[test]
    fn parse_rejects_a_missing_endpoint() {
        let err = BucketConfig::parse(&value(
            r#"
            bucket = "dragon"
            region = "us-east-1"
            access_key_id = "dragon"
            secret_access_key = "dragonsecret"
            "#,
        ))
        .expect_err("missing endpoint");
        assert!(err.to_string().contains("endpoint"), "{err}");
    }

    #[test]
    fn parse_rejects_an_empty_secret() {
        let err = BucketConfig::parse(&value(
            r#"
            bucket = "dragon"
            region = "us-east-1"
            endpoint = "http://127.0.0.1:9000"
            access_key_id = "dragon"
            secret_access_key = ""
            "#,
        ))
        .expect_err("empty secret");
        assert!(err.to_string().contains("secret_access_key"), "{err}");
    }

    #[test]
    fn debug_hides_the_secret() {
        let cfg = BucketConfig::parse(&value(
            r#"
            bucket = "dragon"
            region = "us-east-1"
            endpoint = "http://127.0.0.1:9000"
            access_key_id = "dragon"
            secret_access_key = "dragonsecret"
            "#,
        ))
        .expect("config");
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("dragonsecret"), "{printed}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }
}
