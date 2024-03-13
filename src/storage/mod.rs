mod local;
mod s3;
mod stats;

use async_trait::async_trait;

use crate::{
    error::{Error, Result},
    hash::Hash,
};

use self::stats::StorageStats;

pub use {local::LocalStorage, s3::S3Storage};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;

#[async_trait]
pub trait Storage {
    async fn exists(&mut self, key: &str) -> Result<bool>;
    async fn keys(&mut self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&mut self, key: &str) -> Result<Vec<u8>>;
    async fn put(&mut self, key: &str, bytes: Vec<u8>) -> Result<()>;

    fn stats(&self) -> &StorageStats;
    fn stats_mut(&mut self) -> &mut StorageStats;

    async fn try_get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.get(key).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(Error::ItemNotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

pub const ARCHIVE_KEY_LATEST: &str = "archive:latest";
pub const REF_COUNTS_KEY: &str = "ref-counts";

pub fn archive_key(timestamp: &str) -> String {
    format!("archive:{timestamp}")
}

pub fn block_key(hash: &Hash) -> String {
    format!("block:{hash}")
}
