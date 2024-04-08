use std::sync::Arc;

use tokio::{sync::RwLock, try_join};

use crate::{
    archive::{Archive, ArchiveRecords},
    block::{Block, BlockRecords},
    entity::{Entity, EntityIndex},
    error::Result,
    hash::Hash,
    ops::cleanup::{
        delete::delete_entities,
        find::{find_archives_and_garbage_blocks, find_garbage_entities},
    },
    stats::CommandStats,
    storage::{Storage, MAX_KEYS_PER_REQUEST},
};

mod delete;
mod find;

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
        Box::pin(find_archives_and_garbage_blocks(
            state.clone(),
            hashes,
            archive_sender,
            block_sender
        )),
        delete_entities(state.clone(), archive_receiver),
        delete_entities(state.clone(), block_receiver),
    )?;
    Ok(())
}

pub async fn cleanup_archives(state: Arc<CleanupState>) -> Result<()> {
    let (sender, receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    try_join!(
        find_garbage_entities(state.clone(), state.archive_records.clone(), sender),
        delete_entities(state.clone(), receiver),
    )?;
    Ok(())
}

pub async fn cleanup_blocks(state: Arc<CleanupState>) -> Result<()> {
    let (sender, receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    try_join!(
        find_garbage_entities(state.clone(), state.block_records.clone(), sender),
        delete_entities(state.clone(), receiver),
    )?;
    Ok(())
}
