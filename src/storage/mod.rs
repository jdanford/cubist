mod local;
mod s3;
mod stats;
mod url;

use std::{fmt::Debug, sync::MutexGuard};

use async_trait::async_trait;

use crate::{
    error::{Error, Result},
    prefix::{find_one_by_prefix, longest_common_prefix},
};

pub use {local::LocalStorage, s3::S3Storage, stats::StorageStats, url::StorageUrl};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;

#[async_trait]
pub trait Storage: Debug {
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn delete_many(&self, keys: Vec<String>) -> Result<()>;

    fn stats(&self) -> MutexGuard<StorageStats>;

    async fn try_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.get(key).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(Error::ItemNotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn expand_key(&self, prefix: &str) -> Result<String> {
        let keys = self.keys(Some(prefix)).await?;
        match &keys[..] {
            [key] => Ok(key.clone()),
            [] => Err(Error::NoItemForPrefix(prefix.to_owned())),
            _ => Err(Error::MultipleItemsForPrefix(prefix.to_owned())),
        }
    }

    async fn expand_keys(&self, prefixes: &[&str]) -> Result<Vec<String>> {
        match prefixes {
            [prefix] => return self.expand_key(prefix).await.map(|key| vec![key]),
            [] => return Ok(vec![]),
            _ => {}
        };

        let common_prefix = longest_common_prefix(prefixes);
        let keys = self.keys(common_prefix).await?;
        let mut matching_keys = vec![];

        for prefix in prefixes {
            let matching_key = find_one_by_prefix(&keys, prefix)?;
            matching_keys.push(matching_key.to_owned());
        }

        Ok(matching_keys)
    }
}
