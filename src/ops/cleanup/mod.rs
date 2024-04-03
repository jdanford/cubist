mod archives;
mod blocks;

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{archive::ArchiveRecords, block::BlockRecords, stats::CommandStats, storage::Storage};

pub use self::{archives::cleanup_archives, blocks::cleanup_blocks};

#[derive(Debug)]
pub struct CleanupState {
    pub task_count: usize,
    pub dry_run: bool,
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<Storage>,
    pub archive_records: Arc<RwLock<ArchiveRecords>>,
    pub block_records: Arc<RwLock<BlockRecords>>,
}
