use std::sync::Arc;

use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::Archive,
    compression::{compress, decompress},
    error::Result,
    hash::Hash,
    keys,
    serde::{deserialize, serialize},
    storage::Storage,
};

const COMPRESSION_LEVEL: u8 = 3;

pub async fn download_archive(storage: Arc<Storage>, hash: &Hash) -> Result<Archive> {
    let key = keys::archive(hash);
    let compressed_bytes = storage.get(&key).await?;
    spawn_blocking(move || {
        let bytes = decompress(&compressed_bytes)?;
        deserialize(&bytes)
    })
    .await?
}

pub async fn upload_archive(
    storage: Arc<Storage>,
    hash: &Hash,
    archive: Arc<RwLock<Archive>>,
) -> Result<()> {
    let key = keys::archive(hash);
    let archive_bytes = spawn_blocking(move || {
        let bytes = serialize(&*archive.blocking_read())?;
        let compressed_bytes = compress(&bytes, COMPRESSION_LEVEL)?;
        Result::Ok(compressed_bytes)
    })
    .await??;
    storage.put(&key, archive_bytes).await
}
