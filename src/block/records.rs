use std::{cmp::Ordering, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::{
    entity::{EntityIndex, EntityRecord},
    error::{Error, Result},
    hash::Hash,
};

use super::Block;

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRefs {
    refs: HashMap<Hash<Block>, u64>,
}

impl BlockRefs {
    pub fn new() -> Self {
        BlockRefs {
            refs: HashMap::new(),
        }
    }

    pub fn add_count(&mut self, hash: &Hash<Block>, count: u64) {
        self.refs
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

impl EntityRecord<Block> for BlockRecord {
    fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRecords {
    records: HashMap<Hash<Block>, BlockRecord>,
}

impl BlockRecords {
    pub fn new() -> Self {
        BlockRecords {
            records: HashMap::new(),
        }
    }

    pub fn remove_refs(&mut self, refs: &BlockRefs) -> Result<Vec<(Hash<Block>, BlockRecord)>> {
        let mut removed = vec![];

        for (&hash, &ref_count) in &refs.refs {
            let record = self
                .get_mut(&hash)
                .ok_or_else(|| Error::BlockRecordNotFound(hash))?;
            match record.ref_count.cmp(&ref_count) {
                Ordering::Greater => {
                    record.ref_count -= ref_count;
                }
                Ordering::Equal => {
                    let record = self.records.remove(&hash).unwrap();
                    removed.push((hash, record));
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

impl Default for BlockRecords {
    fn default() -> Self {
        Self::new()
    }
}

impl EntityIndex<Block> for BlockRecords {
    type Record = BlockRecord;

    const KEY: &'static str = "metadata/blocks";

    fn len(&self) -> usize {
        self.records.len()
    }

    fn contains(&self, hash: &Hash<Block>) -> bool {
        self.records.contains_key(hash)
    }

    fn get(&self, hash: &Hash<Block>) -> Option<&BlockRecord> {
        self.records.get(hash)
    }

    fn get_mut(&mut self, hash: &Hash<Block>) -> Option<&mut BlockRecord> {
        self.records.get_mut(hash)
    }

    fn insert(&mut self, hash: Hash<Block>, record: BlockRecord) {
        self.records.insert(hash, record);
    }

    fn remove(&mut self, hash: &Hash<Block>) -> Result<BlockRecord> {
        self.records
            .remove(hash)
            .ok_or_else(|| Error::BlockRecordNotFound(*hash))
    }
}
