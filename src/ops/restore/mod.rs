mod blocks;
mod files;

use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    archive::Archive, block::Block, file::WalkOrder, hash::Hash, locks::BlockLocks,
    stats::CommandStats, storage::Storage,
};

use self::blocks::LocalBlock;

pub use self::files::{download_pending_files, restore_all};

type LocalBlocks = HashMap<Hash<Block>, LocalBlock>;

#[derive(Debug)]
pub struct RestoreState {
    pub order: WalkOrder,
    pub task_count: usize,
    pub dry_run: bool,
    pub archive: Archive,
    pub stats: Arc<RwLock<CommandStats>>,
    pub storage: Arc<Storage>,
    pub local_blocks: Arc<RwLock<LocalBlocks>>,
    pub block_locks: Arc<RwLock<BlockLocks>>,
}
