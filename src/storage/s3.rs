use async_trait::async_trait;
use aws_sdk_s3::{
    error::SdkError,
    operation::{get_object::GetObjectError, head_object::HeadObjectError},
    types::{Delete, ObjectIdentifier},
    Client,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use tokio::task::spawn_blocking;

use crate::error::{Error, Result};

use super::{stats::StorageStats, Storage};

#[derive(Debug)]
pub struct S3Storage {
    client: Client,
    bucket: String,
    stats: StorageStats,
}

impl S3Storage {
    pub async fn new(bucket: String) -> Self {
        let s3_config = aws_config::load_from_env().await;
        let client = Client::new(&s3_config);
        let stats = StorageStats::new();

        S3Storage {
            client,
            bucket,
            stats,
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn exists(&mut self, key: &str) -> Result<bool> {
        let response_result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(SdkError::into_service_error);

        let exists = match response_result {
            Ok(response) => {
                if response.request_charged.is_some() {
                    self.stats.get_requests += 1;
                }

                Ok(true)
            }
            Err(HeadObjectError::NotFound(_)) => {
                self.stats.get_requests += 1;
                Ok(false)
            }
            Err(err) => Err(Error::Sdk(err.to_string())),
        }?;

        Ok(exists)
    }

    async fn keys(&mut self, prefix: Option<&str>) -> Result<Vec<String>> {
        let prefix_owned = prefix.map(ToOwned::to_owned);
        let mut stream = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_prefix(prefix_owned)
            .into_paginator()
            .send();

        let mut keys = vec![];
        let mut charged = false;

        while let Some(page) = stream.try_next().await? {
            charged = charged || page.request_charged.is_some();
            let contents = page.contents.unwrap_or(vec![]);

            for object in contents {
                if let Some(size_signed) = object.size() {
                    let size = u64::try_from(size_signed).unwrap();
                    self.stats.bytes_downloaded += size;
                }

                let key = object.key.ok_or_else(|| Error::InvalidKey(String::new()))?;
                keys.push(key);
            }
        }

        if charged {
            self.stats.get_requests += 1;
        }

        Ok(keys)
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| match err.into_service_error() {
                GetObjectError::NoSuchKey(_) => Error::ItemNotFound(key.to_owned()),
                err => Error::Sdk(err.to_string()),
            })?;

        let bytes = response.body.collect().await?.to_vec();
        self.stats.bytes_downloaded += bytes.len() as u64;

        if response.request_charged.is_some() {
            self.stats.get_requests += 1;
        }

        Ok(bytes)
    }

    async fn put(&mut self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let size = bytes.len() as u64;
        let (bytes, encoded_digest) = spawn_blocking(move || {
            let encoded_digest = md5_base64(&bytes);
            (bytes, encoded_digest)
        })
        .await?;

        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .content_md5(encoded_digest)
            .send()
            .await?;

        self.stats.bytes_uploaded += size;

        if response.request_charged.is_some() {
            self.stats.put_requests += 1;
        }

        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        let response = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        if response.request_charged.is_some() {
            self.stats.put_requests += 1;
        }

        Ok(())
    }

    async fn delete_many(&mut self, keys: Vec<String>) -> Result<()> {
        let delete_objects = keys
            .into_iter()
            .map(|key| {
                ObjectIdentifier::builder()
                    .set_key(Some(key))
                    .build()
                    .map_err(Error::from)
            })
            .collect::<Result<Vec<_>>>()?;

        let delete = Delete::builder()
            .set_objects(Some(delete_objects))
            .quiet(true)
            .build()?;

        let response = self
            .client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await?;

        // self.stats.bytes_deleted = ???;

        if response.request_charged.is_some() {
            self.stats.put_requests += 1;
        }

        Ok(())
    }

    fn stats(&self) -> &StorageStats {
        &self.stats
    }
}

fn md5_base64(bytes: &[u8]) -> String {
    let digest = md5::compute(bytes);
    BASE64_STANDARD.encode(digest.0)
}
