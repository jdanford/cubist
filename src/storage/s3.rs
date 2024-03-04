use async_trait::async_trait;
use aws_sdk_s3::Client;

use crate::error::Result;

use super::core::Storage;

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

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
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
