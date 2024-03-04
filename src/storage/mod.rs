mod core;
mod local;
mod s3;

use crate::hash::Hash;

use self::core::Storage;
pub use {local::LocalStorage, s3::S3Storage};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;

pub const ARCHIVE_KEY_LATEST: &str = "archive:latest";

pub fn archive_key(timestamp: &str) -> String {
    format!("archive:{timestamp}")
}

pub fn block_key(hash: &Hash) -> String {
    format!("block:{hash}")
}
