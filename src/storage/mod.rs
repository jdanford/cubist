mod local;
mod s3;

use async_trait::async_trait;

use crate::{error::Result, hash::Hash};

pub use {local::LocalStorage, s3::S3Storage};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;

#[async_trait]
pub trait Storage {
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
}

pub const ARCHIVE_KEY_LATEST: &str = "archive:latest";

pub const REF_COUNTS_KEY: &str = "ref-counts";

pub fn archive_key(timestamp: &str) -> String {
    format!("archive:{timestamp}")
}

pub fn block_key(hash: &Hash) -> String {
    format!("block:{hash}")
}
