use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    hash::Hash,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRefs {
    map: HashMap<Hash, u64>,
}

impl BlockRefs {
    pub fn new() -> Self {
        BlockRefs {
            map: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn unique_count(&self) -> usize {
        self.map.len()
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.map.contains_key(hash)
    }

    pub fn add_count(&mut self, hash: &Hash, count: u64) {
        self.map
            .entry(*hash)
            .and_modify(|lhs_count| *lhs_count += count)
            .or_insert(count);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRecord {
    pub ref_count: u64,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRecords {
    map: HashMap<Hash, BlockRecord>,
}

impl BlockRecords {
    pub fn new() -> Self {
        BlockRecords {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, hash: &Hash) -> Option<&BlockRecord> {
        self.map.get(hash)
    }

    pub fn get_mut(&mut self, hash: &Hash) -> Option<&mut BlockRecord> {
        self.map.get_mut(hash)
    }

    pub fn insert(&mut self, hash: Hash, record: BlockRecord) {
        self.map.insert(hash, record);
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.map.contains_key(hash)
    }

    pub fn remove_refs(&mut self, refs: &BlockRefs) -> Result<HashSet<Hash>> {
        let mut removed = HashSet::new();

        for (&hash, &ref_count) in &refs.map {
            let record = self
                .get_mut(&hash)
                .ok_or_else(|| Error::BlockRecordNotFound(hash))?;
            match record.ref_count.cmp(&ref_count) {
                Ordering::Greater => {
                    record.ref_count -= ref_count;
                }
                Ordering::Equal => {
                    self.map.remove(&hash);
                    removed.insert(hash);
                }
                Ordering::Less => {
                    return Err(Error::WrongRefCount {
                        hash,
                        actual: record.ref_count,
                        expected: ref_count,
                    });
                }
            }
        }

        Ok(removed)
    }
}
