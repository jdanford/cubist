mod blocks;
mod files;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use blake3::Hash;
use tokio::sync::RwLock;

use crate::{
    archive::Archive, file::WalkOrder, locks::BlockLocks, stats::CommandStats, storage::Storage,
};

use self::blocks::LocalBlock;

pub use self::files::{download_pending_files, restore_recursive};

#[derive(Debug)]
pub struct RestoreState {
    pub archive: Archive,
    pub paths: Vec<PathBuf>,
    pub order: WalkOrder,
    pub task_count: usize,
    pub dry_run: bool,
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<Storage>,
    pub local_blocks: Arc<RwLock<HashMap<Hash, LocalBlock>>>,
    pub block_locks: Arc<RwLock<BlockLocks>>,
}
