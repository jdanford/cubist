mod archive;
mod metadata;
mod node;

use std::{io, path::Path};

use tokio::fs;

use crate::{error::Result, hash::Hash};

use self::metadata::Metadata;
pub use self::{archive::Archive, node::Node};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct File;

pub type FileHash = Hash<File>;

impl FileHash {
    pub fn key(&self) -> String {
        format!("file:{}", self)
    }
}

pub async fn read_metadata(path: &Path) -> Result<Metadata> {
    let native_metadata = fs::symlink_metadata(path).await?;
    let metadata = Metadata::from_native(native_metadata);
    Ok(metadata)
}

pub async fn try_exists<P: AsRef<Path>>(path: P) -> Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
