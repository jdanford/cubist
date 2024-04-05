use std::collections::{BTreeMap, HashMap};

use blake3::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveRecord {
    pub created: DateTime<Utc>,
    pub size: u64,
}

#[derive(Debug)]
pub struct ArchiveRecords {
    records: HashMap<Hash, ArchiveRecord>,
    by_created: BTreeMap<DateTime<Utc>, Hash>,
}

impl ArchiveRecords {
    pub fn new() -> Self {
        ArchiveRecords {
            records: HashMap::new(),
            by_created: BTreeMap::new(),
        }
    }

    pub fn from_records(records: HashMap<Hash, ArchiveRecord>) -> Self {
        let mut by_created = BTreeMap::new();

        for (hash, record) in &records {
            by_created.insert(record.created, *hash);
        }

        ArchiveRecords {
            records,
            by_created,
        }
    }

    #[allow(dead_code)]
    pub fn keys(&self) -> impl Iterator<Item = &Hash> {
        self.records.keys()
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.records.contains_key(hash)
    }

    #[allow(dead_code)]
    pub fn get(&self, hash: &Hash) -> Option<&ArchiveRecord> {
        self.records.get(hash)
    }

    pub fn insert(&mut self, hash: Hash, record: ArchiveRecord) {
        self.by_created.insert(record.created, hash);
        self.records.insert(hash, record);
    }

    pub fn remove(&mut self, hash: &Hash) -> Result<ArchiveRecord> {
        let record = self
            .records
            .remove(hash)
            .ok_or_else(|| Error::ArchiveRecordNotFound(*hash))?;
        self.by_created.remove(&record.created);
        Ok(record)
    }

    pub fn iter_by_created(&self) -> impl Iterator<Item = (&Hash, &ArchiveRecord)> {
        self.by_created.values().map(|hash| {
            let record = self.records.get(hash).unwrap();
            (hash, record)
        })
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
