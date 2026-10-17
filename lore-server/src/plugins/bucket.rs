// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT
//! Bucket immutable-store plugin.
//!
//! Selected with `mode = "bucket"` and configured from `[plugins.bucket]`.
//! The store is the composite durable tier. The local tier keeps the cache cap.

use std::sync::Arc;

use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_storage::ImmutableStore;
use tracing::info;

use crate::plugins::ImmutableStorePluginFactory;
use crate::plugins::PluginError;
use crate::plugins::PluginRegistry;

const PLUGIN_NAME: &str = "bucket";

/// Factory for the S3-compatible durable store.
pub struct BucketImmutableStorePluginFactory;

impl ImmutableStorePluginFactory for BucketImmutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        lore_bucket::BucketConfig::parse(config)
            .map(|_| ())
            .map_err(|err| {
                PluginError::from(PluginConfigError {
                    plugin_name: PLUGIN_NAME.to_string(),
                    message: err.to_string(),
                })
            })
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, PluginError> {
        let plugin_name = self.name();
        info!(plugin_name, "Creating bucket immutable store");

        // The factory trait is synchronous and runs once at startup. Connecting
        // to the bucket is async, so this one call waits here.
        #[allow(clippy::disallowed_methods)]
        let store = tokio::task::block_in_place(|| {
            runtime().block_on(async {
                lore_bucket::open(config).await.map_err(|err| match err {
                    lore_bucket::BucketError::Config(message) => {
                        PluginError::from(PluginConfigError {
                            plugin_name: plugin_name.to_string(),
                            message,
                        })
                    }
                    lore_bucket::BucketError::Init(message) => PluginError::from(PluginInitError {
                        plugin_name: plugin_name.to_string(),
                        message,
                    }),
                })
            })
        })?;
        Ok(store)
    }
}

/// Register the bucket immutable store under the name `bucket`.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(BucketImmutableStorePluginFactory));
}
