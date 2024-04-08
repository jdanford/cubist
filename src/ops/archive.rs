use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::{Archive, ArchiveRecord},
    compress::{compress, decompress},
    error::Result,
    hash::Hash,
    serde::{deserialize, serialize},
    storage::Storage,
};

const COMPRESSION_LEVEL: u8 = 3;

pub async fn download_archive(storage: Arc<Storage>, hash: &Hash<Archive>) -> Result<Archive> {
    let compressed_bytes = storage.get(&hash.key()).await?;
    spawn_blocking(move || {
        let bytes = decompress(&compressed_bytes)?;
        deserialize(&bytes)
    })
    .await?
}

pub async fn upload_archive(
    storage: Arc<Storage>,
    archive: Arc<RwLock<Archive>>,
    created: DateTime<Utc>,
) -> Result<(Hash<Archive>, ArchiveRecord)> {
    let (compressed_bytes, hash, record) = spawn_blocking(move || {
        let bytes = serialize(&*archive.blocking_read())?;
        let compressed_bytes = compress(&bytes, COMPRESSION_LEVEL)?;
        let size = compressed_bytes.len() as u64;
        let record = ArchiveRecord { created, size };
        let hash = Hash::archive(&record);
        Result::Ok((compressed_bytes, hash, record))
    })
    .await??;

    storage.put(&hash.key(), compressed_bytes).await?;
    Ok((hash, record))
}
