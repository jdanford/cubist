use std::sync::Arc;

use tokio::{
    spawn,
    sync::{RwLock, Semaphore},
    task::spawn_blocking,
};

use crate::{
    archive::Archive,
    block::BlockRecords,
    error::Result,
    hash::Hash,
    serde::{deserialize, serialize},
    stats::CoreStats,
    storage::{self, BoxedStorage},
};

pub async fn download_archive(
    storage: Arc<RwLock<BoxedStorage>>,
    archive_name: &str,
) -> Result<Archive> {
    let key = storage::archive_key(archive_name);
    let archive_bytes = storage.write().await.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
}

pub async fn download_archives<S: ToString, I: IntoIterator<Item = S>>(
    storage: Arc<RwLock<BoxedStorage>>,
    names: I,
    max_concurrency: u32,
) -> Result<Vec<Archive>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));
    let archives = Arc::new(RwLock::new(vec![]));

    for name in names {
        let storage = storage.clone();
        let archives = archives.clone();
        let name = name.to_string();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            let archive = download_archive(storage.clone(), &name).await.unwrap();
            archives.write().await.push(archive);
            drop(permit);
        });
    }

    let _ = semaphore.acquire_many(max_concurrency).await.unwrap();
    let archives = Arc::try_unwrap(archives).unwrap().into_inner();
    Ok(archives)
}

pub async fn upload_archive(
    storage: Arc<RwLock<BoxedStorage>>,
    archive: Arc<Archive>,
    stats: &CoreStats,
) -> Result<()> {
    let timestamp = stats.start_time.format("%Y%m%d%H%M%S").to_string();
    let key = storage::archive_key(&timestamp);
    let archive_bytes = spawn_blocking(move || serialize(archive.as_ref())).await??;
    storage.write().await.put(&key, archive_bytes).await?;
    Ok(())
}

pub async fn delete_archive(storage: Arc<RwLock<BoxedStorage>>, name: &str) -> Result<()> {
    let key = storage::archive_key(name);
    storage.write().await.delete(&key).await?;
    Ok(())
}

pub async fn delete_archives<S: ToString, I: IntoIterator<Item = S>>(
    storage: Arc<RwLock<BoxedStorage>>,
    names: I,
    max_concurrency: u32,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));

    for name in names {
        let storage = storage.clone();
        let name = name.to_string();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            delete_archive(storage.clone(), &name).await.unwrap();
            drop(permit);
        });
    }

    let _ = semaphore.acquire_many(max_concurrency).await.unwrap();
    Ok(())
}

pub async fn download_block_records(storage: Arc<RwLock<BoxedStorage>>) -> Result<BlockRecords> {
    let maybe_bytes = storage
        .write()
        .await
        .try_get(storage::BLOCK_RECORDS_KEY)
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
    block_records: BlockRecords,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&block_records)).await??;
    storage
        .write()
        .await
        .put(storage::BLOCK_RECORDS_KEY, bytes)
        .await?;
    Ok(())
}

pub async fn delete_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
) -> Result<()> {
    let keys = hashes.into_iter().map(storage::block_key).collect();
    storage.write().await.delete_many(keys).await?;
    Ok(())
}
