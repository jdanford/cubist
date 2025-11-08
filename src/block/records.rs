use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map},
};

use serde::{Deserialize, Serialize};

use crate::{
    entity::{EntityIndex, EntityRecord},
    error::{Error, Result},
    hash::Hash,
};

use super::Block;

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRefs {
    inner: HashMap<Hash<Block>, u64>,
}

impl BlockRefs {
    pub fn new() -> Self {
        BlockRefs {
            inner: HashMap::new(),
        }
    }

    pub fn add_count(&mut self, hash: &Hash<Block>, count: u64) {
        self.inner
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

    pub fn remove_refs(&mut self, refs: BlockRefs) -> RemoveRefs<'_> {
        RemoveRefs::new(self, refs)
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

pub struct RemoveRefs<'a> {
    records: &'a mut BlockRecords,
    refs_iter: hash_map::IntoIter<Hash<Block>, u64>,
}

impl<'a> RemoveRefs<'a> {
    fn new(records: &'a mut BlockRecords, refs: BlockRefs) -> Self {
        RemoveRefs {
            records,
            refs_iter: refs.inner.into_iter(),
        }
    }
}

impl Iterator for RemoveRefs<'_> {
    type Item = Result<(Hash<Block>, BlockRecord)>;

    fn next(&mut self) -> Option<Self::Item> {
        for (hash, ref_count) in self.refs_iter.by_ref() {
            if let Some(record) = self.records.get_mut(&hash) {
                match record.ref_count.cmp(&ref_count) {
                    Ordering::Greater => {
                        record.ref_count -= ref_count;
                    }
                    Ordering::Equal => {
                        let record = self.records.remove(&hash).unwrap();
                        return Some(Ok((hash, record)));
                    }
                    Ordering::Less => {
                        return Some(Err(Error::WrongRefCount {
                            hash,
                            actual: record.ref_count,
                            expected: ref_count,
                        }));
                    }
                }
            } else {
                return Some(Err(Error::BlockRecordNotFound(hash)));
            }
        }

        None
    }
}
