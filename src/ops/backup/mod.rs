mod blocks;
mod files;

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    archive::Archive, block::BlockRecords, locks::BlockLocks, stats::CommandStats, storage::Storage,
};

pub use self::files::{backup_all, upload_pending_files};

#[derive(Debug)]
pub struct BackupState {
    pub compression_level: u8,
    pub target_block_size: u32,
    pub task_count: usize,
    pub dry_run: bool,
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<Storage>,
    pub archive: Arc<RwLock<Archive>>,
    pub block_records: Arc<RwLock<BlockRecords>>,
    pub block_locks: Arc<RwLock<BlockLocks>>,
}
