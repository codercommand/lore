// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;

use crate::client::ObjectClient;
use crate::client::ObjectError;

/// In-memory object client for tests.
#[derive(Debug, Default)]
pub struct MemoryClient {
    objects: DashMap<String, Bytes>,
}

impl MemoryClient {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl ObjectClient for MemoryClient {
    async fn put(&self, key: &str, body: Bytes) -> Result<(), ObjectError> {
        self.objects.insert(key.to_string(), body);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectError> {
        self.objects
            .get(key)
            .map(|entry| entry.clone())
            .ok_or_else(|| ObjectError::NotFound {
                key: key.to_string(),
            })
    }

    async fn head(&self, key: &str) -> Result<bool, ObjectError> {
        Ok(self.objects.contains_key(key))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        self.objects.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
        let mut keys = self
            .objects
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }
}
