use std::sync::Arc;

use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::ArchiveRecords,
    block::BlockRecords,
    error::Result,
    serde::{deserialize, serialize},
    storage::Storage,
};

const ARCHIVE_RECORDS_KEY: &str = "metadata/archives";
const BLOCK_RECORDS_KEY: &str = "metadata/blocks";

pub async fn download_archive_records(storage: Arc<Storage>) -> Result<ArchiveRecords> {
    let maybe_bytes = storage.try_get(ARCHIVE_RECORDS_KEY).await?;

    let archive_records = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        ArchiveRecords::new()
    };

    Ok(archive_records)
}

pub async fn upload_archive_records(
    storage: Arc<Storage>,
    archive_records: Arc<RwLock<ArchiveRecords>>,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&*archive_records.blocking_read())).await??;
    storage.put(ARCHIVE_RECORDS_KEY, bytes).await
}

pub async fn download_block_records(storage: Arc<Storage>) -> Result<BlockRecords> {
    let maybe_bytes = storage.try_get(BLOCK_RECORDS_KEY).await?;

    let block_records = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        BlockRecords::new()
    };

    Ok(block_records)
}

pub async fn upload_block_records(
    storage: Arc<Storage>,
    block_records: Arc<RwLock<BlockRecords>>,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&*block_records.blocking_read())).await??;
    storage.put(BLOCK_RECORDS_KEY, bytes).await
}
