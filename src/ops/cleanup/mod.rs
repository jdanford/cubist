mod archives;
mod blocks;

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{archive::ArchiveRecords, block::BlockRecords, stats::CommandStats, storage::Storage};

pub use self::{archives::cleanup_archives, blocks::cleanup_blocks};

#[derive(Debug)]
pub struct CleanupArgs {
    pub tasks: usize,
    pub dry_run: bool,
}

#[derive(Debug)]
pub struct CleanupState {
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<RwLock<Storage>>,
    pub archive_records: Arc<RwLock<ArchiveRecords>>,
    pub block_records: Arc<RwLock<BlockRecords>>,
}
