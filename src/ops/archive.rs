use std::sync::Arc;

use tokio::{
    sync::{RwLock, Semaphore},
    task::{spawn_blocking, JoinSet},
};

use crate::{
    arc::{rwarc, unrwarc},
    archive::Archive,
    compression::{compress, decompress},
    error::Result,
    hash::Hash,
    keys,
    serde::{deserialize, serialize},
    storage::BoxedStorage,
};

const COMPRESSION_LEVEL: u8 = 3;

pub async fn download_archive(storage: Arc<RwLock<BoxedStorage>>, hash: &Hash) -> Result<Archive> {
    let key = keys::archive(hash);
    let compressed_bytes = storage.write().await.get(&key).await?;
    let archive = spawn_blocking(move || {
        let bytes = decompress(&compressed_bytes)?;
        deserialize(&bytes)
    })
    .await??;
    Ok(archive)
}

pub async fn download_archives<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
    tasks: usize,
) -> Result<Vec<(Hash, Archive)>> {
    let archives = rwarc(vec![]);
    let semaphore = Arc::new(Semaphore::new(tasks));
    let mut tasks = JoinSet::new();

    for hash in hashes {
        let storage = storage.clone();
        let archives = archives.clone();
        let hash = hash.to_owned();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            let archive = download_archive(storage.clone(), &hash).await?;
            archives.write().await.push((hash, archive));
            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(unrwarc(archives))
}

pub async fn upload_archive(
    storage: Arc<RwLock<BoxedStorage>>,
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
    storage.write().await.put(&key, archive_bytes).await?;
    Ok(())
}

async fn delete_archive(storage: Arc<RwLock<BoxedStorage>>, hash: &Hash) -> Result<()> {
    let key = keys::archive(hash);
    storage.write().await.delete(&key).await?;
    Ok(())
}

pub async fn delete_archives<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
    tasks: usize,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(tasks));
    let mut tasks = JoinSet::new();

    for hash in hashes {
        let storage = storage.clone();
        let hash = hash.to_owned();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            delete_archive(storage.clone(), &hash).await?;
            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
