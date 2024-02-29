use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Storage {
    async fn exists(&self, bucket: &str, key: &str) -> Result<bool>;
    async fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>>;
    async fn put(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()>;
}
