mod metadata;
mod node;

use std::{io, path::Path};

use tokio::fs;

use crate::error::Result;

pub use self::{
    metadata::Metadata,
    node::{FileType, Node},
};

pub async fn read_metadata(path: &Path) -> Result<Metadata> {
    let native_metadata = fs::symlink_metadata(path).await?;
    let metadata = Metadata::from_native(&native_metadata);
    Ok(metadata)
}

pub async fn try_exists<P: AsRef<Path>>(path: P) -> Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
