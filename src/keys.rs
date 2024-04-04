use std::fmt::Display;

use crate::{
    error::{Error, Result},
    hash::Hash,
};

pub const ARCHIVE_NAMESPACE: &str = "archives/";
pub const BLOCK_NAMESPACE: &str = "blocks/";

pub const ARCHIVE_RECORDS_KEY: &str = "metadata/archives";
pub const BLOCK_RECORDS_KEY: &str = "metadata/blocks";

pub fn archive<T: Display>(hash: &T) -> String {
    format!("{ARCHIVE_NAMESPACE}{hash}")
}

pub fn block<T: Display>(hash: &T) -> String {
    format!("{BLOCK_NAMESPACE}{hash}")
}

pub fn hash_from_key(namespace: &str, key: &str) -> Result<Hash> {
    let hash_str = key.strip_prefix(namespace).unwrap();
    hash_str
        .parse()
        .map_err(|_| Error::InvalidHash(hash_str.to_owned()))
}
