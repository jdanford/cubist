mod metadata;
mod node;

use std::{
    io,
    os::unix::fs::{chown, lchown, PermissionsExt},
    path::Path,
};

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

pub async fn restore_metadata_from_node(path: &Path, node: &Node) -> Result<()> {
    restore_metadata(path, node.metadata(), node.file_type()).await
}

pub async fn restore_metadata(path: &Path, metadata: &Metadata, file_type: FileType) -> Result<()> {
    let owner = Some(metadata.owner);
    let group = Some(metadata.group);
    let permissions = PermissionsExt::from_mode(metadata.mode);

    if file_type.is_symlink() {
        lchown(path, owner, group)?;
    } else {
        chown(path, owner, group)?;
    }

    fs::set_permissions(path, permissions).await?;
    Ok(())
}

pub async fn try_exists<P: AsRef<Path>>(path: P) -> Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}
