use std::{pin::pin, sync::Arc};

use async_channel::Sender;
use tokio::{
    sync::{RwLock, Semaphore},
    task::{block_in_place, JoinSet},
};
use tokio_stream::StreamExt;

use crate::{
    archive::Archive,
    entity::{Entity, EntityIndex},
    error::Result,
    hash::Hash,
    ops::download_archive,
};

use super::{CleanupState, RemovedArchive, RemovedBlock, RemovedEntity};

pub async fn find_garbage_entities<E, I>(
    state: Arc<CleanupState>,
    records: Arc<RwLock<I>>,
    sender: Sender<RemovedEntity<E, I>>,
) -> Result<()>
where
    E: Entity + Send + Sync + 'static,
    I: EntityIndex<E> + Send + Sync + 'static,
    I::Record: Send + Sync,
{
    let semaphore = Arc::new(Semaphore::new(state.task_count));
    let mut tasks = JoinSet::new();

    let mut key_chunks = pin!(state.storage.keys_paginated(Some(E::KEY_PREFIX)));
    while let Some(keys) = key_chunks.try_next().await? {
        let records = records.clone();
        let sender = sender.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn_blocking(move || {
            let records = records.blocking_read();

            for key in keys {
                let hash = Hash::from_key(&key)?;
                if !records.contains(&hash) {
                    let removed_entity = RemovedEntity { hash, record: None };
                    sender.send_blocking(removed_entity)?;
                }
            }

            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

pub async fn find_archives_and_garbage_blocks<'a, I>(
    state: Arc<CleanupState>,
    hashes: I,
    archive_sender: Sender<RemovedArchive>,
    block_sender: Sender<RemovedBlock>,
) -> Result<()>
where
    I: IntoIterator<Item = &'a Hash<Archive>>,
{
    let semaphore = Arc::new(Semaphore::new(state.task_count));
    let mut tasks = JoinSet::new();

    for hash in hashes {
        let state = state.clone();
        let hash = *hash;
        let archive_sender = archive_sender.clone();
        let block_sender = block_sender.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            let archive = download_archive(state.storage.clone(), &hash).await?;
            let record = state.archive_records.write().await.remove(&hash)?;
            let removed_archive = RemovedArchive {
                hash,
                record: Some(record),
            };
            archive_sender.send(removed_archive).await?;

            block_in_place(move || {
                let mut block_records = state.block_records.blocking_write();
                let garbage_blocks = block_records.remove_refs(&archive.block_refs)?;
                for (hash, record) in garbage_blocks {
                    let removed_block = RemovedBlock {
                        hash,
                        record: Some(record),
                    };
                    block_sender.send_blocking(removed_block)?;
                }

                Result::Ok(())
            })?;

            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
