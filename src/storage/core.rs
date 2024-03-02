use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Storage {
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;
}
