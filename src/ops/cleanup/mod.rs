mod archives;
mod blocks;

use std::{mem, sync::Arc};

use async_channel::{Receiver, Sender};
use clap::builder::styling::AnsiColor;
use log::debug;
use tokio::{
    sync::{RwLock, Semaphore},
    task::{spawn_blocking, JoinSet},
    try_join,
};

use crate::{
    archive::{Archive, ArchiveRecords},
    block::{Block, BlockRecords},
    entity::{Entity, EntityIndex, EntityRecord},
    error::Result,
    hash::Hash,
    stats::{CommandStats, EntityStats},
    storage::{Storage, MAX_KEYS_PER_REQUEST},
};

pub use self::{archives::cleanup_archives, blocks::cleanup_blocks};

use super::download_archive;

#[derive(Debug)]
pub struct CleanupState {
    pub task_count: usize,
    pub dry_run: bool,
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<Storage>,
    pub archive_records: Arc<RwLock<ArchiveRecords>>,
    pub block_records: Arc<RwLock<BlockRecords>>,
}

#[derive(Debug)]
pub struct RemovedEntity<E: Entity, I: EntityIndex<E>> {
    pub hash: Hash<E>,
    pub record: Option<I::Record>,
}

type RemovedArchive = RemovedEntity<Archive, ArchiveRecords>;
type RemovedBlock = RemovedEntity<Block, BlockRecords>;

pub async fn delete_archives_and_garbage_blocks<'a, I>(
    state: Arc<CleanupState>,
    hashes: I,
) -> Result<()>
where
    I: IntoIterator<Item = &'a Hash<Archive>>,
{
    let (archive_sender, archive_receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    let (block_sender, block_receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    try_join!(
        find_archives_and_garbage_blocks(state.clone(), hashes, archive_sender, block_sender),
        delete_entities(state.clone(), archive_receiver),
        delete_entities(state.clone(), block_receiver),
    )?;
    Ok(())
}

async fn find_archives_and_garbage_blocks<'a, I>(
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
        let storage = state.storage.clone();
        let hash = *hash;
        let archive_sender = archive_sender.clone();
        let block_sender = block_sender.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            let archive = download_archive(storage.clone(), &hash).await?;
            let record = state.archive_records.write().await.remove(&hash)?;
            let removed_archive = RemovedArchive {
                hash,
                record: Some(record),
            };
            archive_sender.send(removed_archive).await?;

            spawn_blocking(move || {
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
            })
            .await??;

            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

pub async fn delete_entities<E: Entity, I: EntityIndex<E>>(
    state: Arc<CleanupState>,
    receiver: Receiver<RemovedEntity<E, I>>,
) -> Result<()>
where
    CommandStats: EntityStats<E>,
{
    let chunk_size = MAX_KEYS_PER_REQUEST;
    let mut hashes = vec![];
    let mut bytes = 0;

    while let Ok(removed_block) = receiver.recv().await {
        hashes.push(removed_block.hash);

        if let Some(record) = removed_block.record {
            bytes += record.size();
        }

        maybe_delete_chunk(state.clone(), &mut hashes, &mut bytes, chunk_size).await?;
    }

    maybe_delete_chunk(state.clone(), &mut hashes, &mut bytes, 1).await?;
    Ok(())
}

async fn maybe_delete_chunk<E: Entity>(
    state: Arc<CleanupState>,
    hashes: &mut Vec<Hash<E>>,
    bytes: &mut u64,
    chunk_size: usize,
) -> Result<()>
where
    CommandStats: EntityStats<E>,
{
    let count = hashes.len();
    if count >= chunk_size {
        let deleted_hashes = mem::take(hashes);

        if !state.dry_run {
            let deleted_keys = deleted_hashes.iter().map(Hash::key);
            state.storage.delete_many(deleted_keys).await?;
        }

        state.stats.write().await.bytes_deleted += mem::take(bytes);
        state.stats.write().await.add_entities_deleted(count as u64);

        for hash in deleted_hashes {
            let entity_name = E::NAME;
            let style = AnsiColor::Yellow.on_default();
            debug!("{style}deleted {entity_name}{style:#} {hash}");
        }
    }

    Ok(())
}
