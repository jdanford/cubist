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
    let key = key.strip_prefix(namespace).unwrap();
    let archive_hash = key
        .parse()
        .map_err(|_| Error::InvalidHash(key.to_owned()))?;
    Ok(archive_hash)
}
