use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::ArchiveRecords,
    block::BlockRecords,
    entity::{Entity, EntityIndex},
    error::Result,
    serde::{deserialize, serialize},
    storage::Storage,
};

pub async fn download_archive_records(storage: Arc<Storage>) -> Result<ArchiveRecords> {
    download_records(storage).await
}

pub async fn download_block_records(storage: Arc<Storage>) -> Result<BlockRecords> {
    download_records(storage).await
}

pub async fn upload_archive_records(
    storage: Arc<Storage>,
    records: Arc<RwLock<ArchiveRecords>>,
) -> Result<()> {
    upload_records(storage, records).await
}

pub async fn upload_block_records(
    storage: Arc<Storage>,
    records: Arc<RwLock<BlockRecords>>,
) -> Result<()> {
    upload_records(storage, records).await
}

async fn download_records<E, I>(storage: Arc<Storage>) -> Result<I>
where
    E: Entity,
    I: EntityIndex<E> + DeserializeOwned + Default + Send + Sync + 'static,
{
    let maybe_bytes = storage.try_get(I::KEY).await?;
    let archive_records = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        I::default()
    };

    Ok(archive_records)
}

async fn upload_records<E, I>(storage: Arc<Storage>, records: Arc<RwLock<I>>) -> Result<()>
where
    E: Entity,
    I: EntityIndex<E> + Serialize + Send + Sync + 'static,
{
    let bytes = spawn_blocking(move || serialize(&*records.blocking_read())).await??;
    storage.put(I::KEY, bytes).await
}
