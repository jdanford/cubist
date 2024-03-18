use std::{path::PathBuf, str::FromStr};

use crate::error::Error;

pub const S3_PREFIX: &str = "s3://";
pub const LOCAL_PREFIX: &str = "file://";

#[derive(Debug, Clone)]
pub enum StorageUrl {
    S3(String),
    Local(PathBuf),
}

impl FromStr for StorageUrl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(bucket) = s.strip_prefix(S3_PREFIX) {
            Ok(StorageUrl::S3(bucket.to_owned()))
        } else if let Some(path_str) = s.strip_prefix(LOCAL_PREFIX) {
            let path = path_str.into();
            Ok(StorageUrl::Local(path))
        } else {
            Err(Error::InvalidStorageUrl(s.to_owned()))
        }
    }
}
