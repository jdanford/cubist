use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Storage {
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
}
