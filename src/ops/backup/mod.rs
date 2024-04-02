mod blocks;
mod files;

use std::{path::PathBuf, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    archive::Archive, block::BlockRecords, locks::BlockLocks, stats::CommandStats, storage::Storage,
};

pub use files::{backup_recursive, upload_pending_files};

#[derive(Debug)]
pub struct UploadArgs {
    pub paths: Vec<PathBuf>,
    pub compression_level: u8,
    pub target_block_size: u32,
    pub tasks: usize,
    pub dry_run: bool,
}

#[derive(Debug)]
pub struct UploadState {
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<RwLock<Storage>>,
    pub archive: Arc<RwLock<Archive>>,
    pub block_records: Arc<RwLock<BlockRecords>>,
    pub block_locks: Arc<RwLock<BlockLocks>>,
}
