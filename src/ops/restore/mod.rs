mod blocks;
mod files;

use std::{collections::HashMap, sync::Arc};

use blake3::Hash;
use tokio::sync::RwLock;

use crate::{
    archive::Archive, file::WalkOrder, locks::BlockLocks, stats::CommandStats, storage::Storage,
};

use self::blocks::LocalBlock;

pub use self::files::{download_pending_files, restore_all};

#[derive(Debug)]
pub struct RestoreState {
    pub order: WalkOrder,
    pub task_count: usize,
    pub dry_run: bool,
    pub archive: Archive,
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<Storage>,
    pub local_blocks: Arc<RwLock<HashMap<Hash, LocalBlock>>>,
    pub block_locks: Arc<RwLock<BlockLocks>>,
}
