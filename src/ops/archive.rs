use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::{Archive, ArchiveRecord},
    compression::{compress, decompress},
    error::Result,
    hash::{self, Hash},
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
    created: DateTime<Utc>,
    archive: Arc<RwLock<Archive>>,
) -> Result<(Hash, ArchiveRecord)> {
    let compressed_bytes = spawn_blocking(move || {
        let bytes = serialize(&*archive.blocking_read())?;
        compress(&bytes, COMPRESSION_LEVEL)
    })
    .await??;
    let size = compressed_bytes.len() as u64;
    let record = ArchiveRecord { created, size };
    let hash = hash::archive(&record);
    let key = keys::archive(&hash);

    storage.put(&key, compressed_bytes).await?;
    Ok((hash, record))
}
