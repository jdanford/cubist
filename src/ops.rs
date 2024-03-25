use std::sync::Arc;

use tokio::{
    sync::{RwLock, Semaphore},
    task::{spawn_blocking, JoinSet},
};

use crate::{
    arc::{rwarc, unrwarc},
    archive::{Archive, ArchiveRecords},
    block::BlockRecords,
    error::{Error, Result, OK},
    hash::{Hash, ShortHash},
    serde::{deserialize, serialize},
    storage::{self, BoxedStorage},
};

pub async fn download_archive(storage: Arc<RwLock<BoxedStorage>>, hash: &Hash) -> Result<Archive> {
    let key = storage::archive_key(hash);
    let archive_bytes = storage.write().await.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
}

pub async fn download_archives<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
    tasks: usize,
) -> Result<Vec<Archive>> {
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
            archives.write().await.push(archive);
            drop(permit);
            OK
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(unrwarc(archives))
}

pub async fn upload_archive(
    storage: Arc<RwLock<BoxedStorage>>,
    archive: Arc<RwLock<Archive>>,
) -> Result<()> {
    let key = storage::archive_key(&archive.read().await.hash());
    let archive_bytes = spawn_blocking(move || serialize(&*archive.blocking_read())).await??;
    storage.write().await.put(&key, archive_bytes).await?;
    Ok(())
}

pub async fn delete_archive(storage: Arc<RwLock<BoxedStorage>>, hash: &Hash) -> Result<()> {
    let key = storage::archive_key(hash);
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
            OK
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

pub async fn download_archive_records(
    storage: Arc<RwLock<BoxedStorage>>,
) -> Result<ArchiveRecords> {
    let maybe_bytes = storage
        .write()
        .await
        .try_get(storage::ARCHIVE_RECORDS_KEY)
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
        .put(storage::ARCHIVE_RECORDS_KEY, bytes)
        .await?;
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
    block_records: Arc<RwLock<BlockRecords>>,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&*block_records.blocking_read())).await??;
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

pub async fn find_archive_hash(
    storage: Arc<RwLock<BoxedStorage>>,
    short_hash: &ShortHash,
) -> Result<Hash> {
    let partial_key = storage::archive_key(short_hash);
    let namespaced_key = storage.write().await.expand_key(&partial_key).await?;
    archive_hash_from_namespaced_key(&namespaced_key)
}

pub async fn find_archive_hashes(
    storage: Arc<RwLock<BoxedStorage>>,
    short_hashes: &[&ShortHash],
) -> Result<Vec<Hash>> {
    let partial_keys_owned = short_hashes
        .iter()
        .map(storage::archive_key)
        .collect::<Vec<_>>();
    let partial_keys = partial_keys_owned
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let namespaced_keys = storage.write().await.expand_keys(&partial_keys).await?;
    namespaced_keys
        .into_iter()
        .map(|key| archive_hash_from_namespaced_key(key.as_str()))
        .collect()
}

pub fn archive_hash_from_namespaced_key(namespaced_key: &str) -> Result<Hash> {
    let key = namespaced_key
        .strip_prefix(storage::ARCHIVE_KEY_PREFIX)
        .unwrap();
    let archive_hash = key
        .parse()
        .map_err(|_| Error::InvalidHash(key.to_owned()))?;
    Ok(archive_hash)
}
