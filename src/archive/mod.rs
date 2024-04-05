mod records;

use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{
    block::{Block, BlockRefs},
    entity::Entity,
    file::FileTree,
    hash::Hash,
};

pub use self::records::{ArchiveRecord, ArchiveRecords};

#[derive(Debug, Serialize, Deserialize)]
pub struct Archive {
    files: FileTree,
    pub block_refs: BlockRefs,
}

impl Archive {
    pub fn new() -> Self {
        Archive {
            files: FileTree::new(),
            block_refs: BlockRefs::new(),
        }
    }

    pub fn add_ref(&mut self, hash: &Hash<Block>) {
        self.block_refs.add_count(hash, 1);
    }
}

impl Entity for Archive {
    const NAME: &'static str = "archive";
    const KEY_PREFIX: &'static str = "archives/";
}

impl Deref for Archive {
    type Target = FileTree;

    fn deref(&self) -> &Self::Target {
        &self.files
    }
}

impl DerefMut for Archive {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.files
    }
}
