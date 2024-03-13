use async_trait::async_trait;
use aws_sdk_s3::{
    error::SdkError,
    operation::{get_object::GetObjectError, head_object::HeadObjectError},
    Client,
};

use crate::error::{Error, Result};

use super::{stats::StorageStats, Storage};

#[derive(Debug)]
pub struct S3Storage {
    client: aws_sdk_s3::Client,
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
        let head_result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(SdkError::into_service_error);

        let exists = match head_result {
            Ok(_) => Ok(true),
            Err(HeadObjectError::NotFound(_)) => Ok(false),
            Err(err) => Err(Error::Sdk(err.to_string())),
        }?;

        self.stats.get_requests += 1;
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
        while let Some(result) = stream.next().await {
            let page = result?;
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

        self.stats.get_requests += 1;
        Ok(keys)
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        let object = self
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

        let bytes = object.body.collect().await?.to_vec();
        self.stats.bytes_downloaded += bytes.len() as u64;
        self.stats.get_requests += 1;
        Ok(bytes)
    }

    async fn put(&mut self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let size = bytes.len() as u64;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .send()
            .await?;

        self.stats.bytes_uploaded += size;
        self.stats.put_requests += 1;
        Ok(())
    }

    fn stats(&self) -> &StorageStats {
        &self.stats
    }

    fn stats_mut(&mut self) -> &mut StorageStats {
        &mut self.stats
    }
}
