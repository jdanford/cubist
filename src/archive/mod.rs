mod map;

use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{hash::Hash, refs::RefCounts};

use self::map::FileMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Archive {
    files: FileMap,
    pub ref_counts: RefCounts,
}

impl Archive {
    pub fn new() -> Self {
        Archive {
            files: FileMap::new(),
            ref_counts: RefCounts::new(),
        }
    }

    pub fn add_ref(&mut self, hash: &Hash) {
        self.ref_counts.add_count(hash, 1);
    }
}

impl Deref for Archive {
    type Target = FileMap;

    fn deref(&self) -> &Self::Target {
        &self.files
    }
}

impl DerefMut for Archive {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.files
    }
}
