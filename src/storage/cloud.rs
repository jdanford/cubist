use async_trait::async_trait;
use aws_sdk_s3::Client;

use crate::error::Result;

use super::core::Storage;

pub struct CloudStorage {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl CloudStorage {
    pub async fn new(bucket: String) -> Self {
        let s3_config = aws_config::load_from_env().await;
        let client = Client::new(&s3_config);
        CloudStorage { client, bucket }
    }
}

#[async_trait]
impl Storage for CloudStorage {
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
        let data = object.body.collect().await?.to_vec();
        Ok(data)
    }

    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .send()
            .await?;
        Ok(())
    }
}
