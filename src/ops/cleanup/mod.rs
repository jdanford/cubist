mod archives;
mod blocks;
mod both;

use std::sync::Arc;

use blake3::Hash;
use tokio::sync::RwLock;

use crate::{
    archive::{ArchiveRecord, ArchiveRecords},
    block::{BlockRecord, BlockRecords},
    stats::CommandStats,
    storage::Storage,
};

pub use self::{
    archives::cleanup_archives, blocks::cleanup_blocks, both::delete_archives_and_garbage_blocks,
};

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
pub struct RemovedArchive {
    pub hash: Hash,
    pub record: Option<ArchiveRecord>,
}

#[derive(Debug)]
pub struct RemovedBlock {
    pub hash: Hash,
    pub record: Option<BlockRecord>,
}
