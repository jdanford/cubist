use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::{Archive, ArchiveRecord},
    compression::{compress, decompress},
    error::Result,
    hash::Hash,
    serde::{deserialize, serialize},
    storage::Storage,
};

const COMPRESSION_LEVEL: u8 = 3;

pub async fn download_archive(storage: Arc<Storage>, hash: &Hash<Archive>) -> Result<Archive> {
    let key = hash.key();
    let compressed_bytes = storage.get(&key).await?;
    spawn_blocking(move || {
        let bytes = decompress(&compressed_bytes)?;
        deserialize(&bytes)
    })
    .await?
}

pub async fn upload_archive(
    storage: Arc<Storage>,
    created: DateTime<Utc>,
    archive: Arc<RwLock<Archive>>,
) -> Result<(Hash<Archive>, ArchiveRecord)> {
    let compressed_bytes = spawn_blocking(move || {
        let bytes = serialize(&*archive.blocking_read())?;
        compress(&bytes, COMPRESSION_LEVEL)
    })
    .await??;
    let size = compressed_bytes.len() as u64;
    let record = ArchiveRecord { created, size };
    let hash = Hash::archive(&record);
    let key = hash.key();

    storage.put(&key, compressed_bytes).await?;
    Ok((hash, record))
}
