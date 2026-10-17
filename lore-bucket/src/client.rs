// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use async_trait::async_trait;
use bytes::Bytes;

/// Object storage operations the durable tier needs.
///
/// `list` is for obliterate, copy of a zero context, and snapshot restore.
/// Query and get address an exact key.
#[async_trait]
pub trait ObjectClient: Send + Sync {
    async fn put(&self, key: &str, body: Bytes) -> Result<(), ObjectError>;
    async fn get(&self, key: &str) -> Result<Bytes, ObjectError>;
    async fn head(&self, key: &str) -> Result<bool, ObjectError>;
    async fn delete(&self, key: &str) -> Result<(), ObjectError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError>;
}

/// Failure from the object client.
#[derive(Debug, thiserror::Error)]
pub enum ObjectError {
    /// The key is not in the bucket.
    #[error("not found: {key}")]
    NotFound {
        /// Missing key.
        key: String,
    },
    /// Any other client or service failure.
    #[error("{message}")]
    Failed {
        /// What the client reported.
        message: String,
    },
}

impl ObjectError {
    pub fn failed(err: impl std::fmt::Display) -> Self {
        Self::Failed {
            message: err.to_string(),
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }
}

/// Join a bucket prefix and a relative key with a single slash.
pub fn join_key(prefix: &str, rest: &str) -> String {
    if prefix.is_empty() {
        rest.to_string()
    } else {
        format!("{prefix}/{rest}")
    }
}
