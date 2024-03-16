mod local;
mod s3;
mod stats;

use std::fmt::Debug;

use async_trait::async_trait;

use crate::{
    error::{Error, Result},
    hash::Hash,
};

use self::stats::StorageStats;

pub use {local::LocalStorage, s3::S3Storage};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;

#[async_trait]
pub trait Storage: Debug {
    async fn exists(&mut self, key: &str) -> Result<bool>;
    async fn keys(&mut self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&mut self, key: &str) -> Result<Vec<u8>>;
    async fn put(&mut self, key: &str, bytes: Vec<u8>) -> Result<()>;
    async fn delete(&mut self, key: &str) -> Result<()>;
    async fn delete_many(&mut self, keys: Vec<String>) -> Result<()>;

    fn stats(&self) -> &StorageStats;

    async fn try_get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.get(key).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(Error::ItemNotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

pub const ARCHIVE_KEY_PREFIX: &str = "archive:";
pub const BLOCK_KEY_PREFIX: &str = "block:";
pub const REF_COUNTS_KEY: &str = "ref-counts";

pub fn archive_key(name: &str) -> String {
    format!("{ARCHIVE_KEY_PREFIX}{name}")
}

pub fn block_key(hash: &Hash) -> String {
    format!("{BLOCK_KEY_PREFIX}{hash}")
}
