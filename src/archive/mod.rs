mod records;

use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{
    block::BlockRefs,
    file::{FileTree, FileTreeBuilder},
    hash::Hash,
};

pub use self::records::{ArchiveRecord, ArchiveRecords};

#[derive(Debug, Serialize, Deserialize)]
pub struct Archive {
    files: FileTree,
    pub block_refs: BlockRefs,
}

impl Archive {
    pub fn hash(&self) -> &Hash {
        &self.files.hash
    }
}

impl Deref for Archive {
    type Target = FileTree;

    fn deref(&self) -> &Self::Target {
        &self.files
    }
}

#[derive(Debug)]
pub struct ArchiveBuilder {
    files: FileTreeBuilder,
    pub block_refs: BlockRefs,
}

impl ArchiveBuilder {
    pub fn new() -> Self {
        ArchiveBuilder {
            files: FileTree::builder(),
            block_refs: BlockRefs::new(),
        }
    }

    pub fn add_ref(&mut self, hash: &Hash) {
        self.block_refs.add_count(hash, 1);
    }

    pub fn finalize(self) -> Archive {
        let files = self.files.finalize();
        Archive {
            files,
            block_refs: self.block_refs,
        }
    }
}

impl Deref for ArchiveBuilder {
    type Target = FileTreeBuilder;

    fn deref(&self) -> &Self::Target {
        &self.files
    }
}

impl DerefMut for ArchiveBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.files
    }
}
