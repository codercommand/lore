// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_http_client::Builder as HttpClientBuilder;
use aws_smithy_http_client::Connector;
use aws_smithy_http_client::tls;
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use bytes::Bytes;

use crate::BucketError;
use crate::client::ObjectClient;
use crate::client::ObjectError;
use crate::config::BucketConfig;

/// S3-compatible client. TLS uses rustls with the ring provider so the build
/// does not need aws-lc.
pub struct S3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
}

pub async fn connect(config: &BucketConfig) -> Result<Arc<dyn ObjectClient>, BucketError> {
    let http_client = HttpClientBuilder::new()
        .pool_idle_timeout(Duration::from_secs(30))
        .build_with_connector_fn(|_, _| {
            Connector::builder()
                .tls_provider(tls::Provider::Rustls(CryptoMode::Ring))
                .build()
        });

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .http_client(http_client)
        .region(aws_config::Region::new(config.region.clone()));

    if let Some(endpoint) = &config.endpoint
        && !endpoint.is_empty()
    {
        loader = loader.endpoint_url(endpoint);
    }
    if let (Some(key), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
        loader = loader.credentials_provider(Credentials::new(
            key.clone(),
            secret.clone(),
            None,
            None,
            "lore-bucket",
        ));
    }

    let sdk = loader.load().await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk)
        .force_path_style(config.path_style)
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_config);
    client
        .head_bucket()
        .bucket(&config.bucket)
        .send()
        .await
        .map_err(|err| {
            BucketError::Init(format!(
                "bucket '{}' is not reachable: {err}",
                config.bucket
            ))
        })?;

    Ok(Arc::new(S3Client {
        client,
        bucket: config.bucket.clone(),
    }))
}

#[async_trait]
impl ObjectClient for S3Client {
    async fn put(&self, key: &str, body: Bytes) -> Result<(), ObjectError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map(|_| ())
            .map_err(ObjectError::failed)
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectError> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output
                .body
                .collect()
                .await
                .map(|collected| collected.into_bytes())
                .map_err(ObjectError::failed),
            Err(err) if is_missing_get(&err) => Err(ObjectError::NotFound {
                key: key.to_string(),
            }),
            Err(err) => Err(ObjectError::failed(err)),
        }
    }

    async fn head(&self, key: &str) -> Result<bool, ObjectError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if is_missing_head(&err) => Ok(false),
            Err(err) => Err(ObjectError::failed(err)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(ObjectError::failed)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectError> {
        let mut keys = Vec::new();
        let mut token = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = token.as_ref() {
                request = request.continuation_token(token);
            }
            let page = request.send().await.map_err(ObjectError::failed)?;
            if let Some(contents) = page.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        keys.push(key);
                    }
                }
            }
            if page.is_truncated == Some(true) {
                token = page.next_continuation_token;
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(keys)
    }
}

fn is_missing_head(err: &SdkError<HeadObjectError>) -> bool {
    err.as_service_error()
        .is_some_and(|service| service.is_not_found())
}

fn is_missing_get(err: &SdkError<GetObjectError>) -> bool {
    err.as_service_error()
        .is_some_and(|service| service.is_no_such_key())
}
