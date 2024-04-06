use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    entity::{EntityIndex, EntityRecord},
    error::{Error, Result},
    hash::Hash,
};

use super::Archive;

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveRecord {
    pub created: DateTime<Utc>,
    pub size: u64,
}

impl EntityRecord<Archive> for ArchiveRecord {
    fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug)]
pub struct ArchiveRecords {
    records: HashMap<Hash<Archive>, ArchiveRecord>,
    by_created: BTreeMap<DateTime<Utc>, Hash<Archive>>,
}

impl ArchiveRecords {
    pub fn new() -> Self {
        ArchiveRecords {
            records: HashMap::new(),
            by_created: BTreeMap::new(),
        }
    }

    pub fn from_records(records: HashMap<Hash<Archive>, ArchiveRecord>) -> Self {
        let mut by_created = BTreeMap::new();

        for (hash, record) in &records {
            by_created.insert(record.created, *hash);
        }

        ArchiveRecords {
            records,
            by_created,
        }
    }

    pub fn iter_by_created(&self) -> impl Iterator<Item = (&Hash<Archive>, &ArchiveRecord)> {
        self.by_created.values().map(|hash| {
            let record = self.records.get(hash).unwrap();
            (hash, record)
        })
    }
}

impl Default for ArchiveRecords {
    fn default() -> Self {
        Self::new()
    }
}

impl EntityIndex<Archive> for ArchiveRecords {
    type Record = ArchiveRecord;

    const KEY: &'static str = "metadata/archives";

    fn len(&self) -> usize {
        self.records.len()
    }

    fn contains(&self, hash: &Hash<Archive>) -> bool {
        self.records.contains_key(hash)
    }

    fn get(&self, hash: &Hash<Archive>) -> Option<&ArchiveRecord> {
        self.records.get(hash)
    }

    fn get_mut(&mut self, hash: &Hash<Archive>) -> Option<&mut ArchiveRecord> {
        self.records.get_mut(hash)
    }

    fn insert(&mut self, hash: Hash<Archive>, record: ArchiveRecord) {
        self.by_created.insert(record.created, hash);
        self.records.insert(hash, record);
    }

    fn remove(&mut self, hash: &Hash<Archive>) -> Result<ArchiveRecord> {
        let record = self
            .records
            .remove(hash)
            .ok_or_else(|| Error::ArchiveRecordNotFound(*hash))?;
        self.by_created.remove(&record.created);
        Ok(record)
    }
}

impl Serialize for ArchiveRecords {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        Serialize::serialize(&self.records, serializer)
    }
}

impl<'de> Deserialize<'de> for ArchiveRecords {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<ArchiveRecords, D::Error> {
        Deserialize::deserialize(deserializer).map(ArchiveRecords::from_records)
    }
}
