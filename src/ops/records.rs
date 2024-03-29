use std::sync::Arc;

use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::ArchiveRecords,
    block::BlockRecords,
    error::Result,
    keys,
    serde::{deserialize, serialize},
    storage::BoxedStorage,
};

pub async fn download_archive_records(
    storage: Arc<RwLock<BoxedStorage>>,
) -> Result<ArchiveRecords> {
    let maybe_bytes = storage
        .write()
        .await
        .try_get(keys::ARCHIVE_RECORDS_KEY)
        .await?;

    let archive_records = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        ArchiveRecords::new()
    };

    Ok(archive_records)
}

pub async fn upload_archive_records(
    storage: Arc<RwLock<BoxedStorage>>,
    archive_records: Arc<RwLock<ArchiveRecords>>,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&*archive_records.blocking_read())).await??;
    storage
        .write()
        .await
        .put(keys::ARCHIVE_RECORDS_KEY, bytes)
        .await?;
    Ok(())
}

pub async fn download_block_records(storage: Arc<RwLock<BoxedStorage>>) -> Result<BlockRecords> {
    let maybe_bytes = storage
        .write()
        .await
        .try_get(keys::BLOCK_RECORDS_KEY)
        .await?;

    let block_records = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        BlockRecords::new()
    };

    Ok(block_records)
}

pub async fn upload_block_records(
    storage: Arc<RwLock<BoxedStorage>>,
    block_records: Arc<RwLock<BlockRecords>>,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&*block_records.blocking_read())).await??;
    storage
        .write()
        .await
        .put(keys::BLOCK_RECORDS_KEY, bytes)
        .await?;
    Ok(())
}
