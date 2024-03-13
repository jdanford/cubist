use async_trait::async_trait;
use aws_sdk_s3::{operation::get_object::GetObjectError, Client};

use crate::error::{Error, Result};

use super::Storage;

#[derive(Debug)]
pub struct S3Storage {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Storage {
    pub async fn new(bucket: String) -> Self {
        let s3_config = aws_config::load_from_env().await;
        let client = Client::new(&s3_config);
        S3Storage { client, bucket }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn exists(&self, key: &str) -> Result<bool> {
        let head_result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        Ok(head_result.is_ok())
    }

    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
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
            for object in page.contents() {
                let key = object
                    .key()
                    .ok_or_else(|| Error::InvalidKey(String::new()))?;
                keys.push(key.to_owned());
            }
        }

        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
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
        Ok(bytes)
    }

    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .send()
            .await?;
        Ok(())
    }
}
