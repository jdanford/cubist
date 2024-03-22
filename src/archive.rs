use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{block::BlockRefs, file::FileTree, hash::Hash};

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

    pub fn add_ref(&mut self, hash: &Hash) {
        self.block_refs.add_count(hash, 1);
    }
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