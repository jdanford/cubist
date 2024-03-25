use std::collections::{BTreeMap, HashMap, HashSet};

use blake3::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveRecord {
    pub created: DateTime<Utc>,
    pub tags: HashSet<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveRecords {
    records: HashMap<Hash, ArchiveRecord>,
    by_created: BTreeMap<DateTime<Utc>, Hash>,
    by_tag: HashMap<String, HashSet<Hash>>,
}

impl ArchiveRecords {
    pub fn new() -> Self {
        ArchiveRecords {
            records: HashMap::new(),
            by_created: BTreeMap::new(),
            by_tag: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn get(&self, hash: &Hash) -> Option<&ArchiveRecord> {
        self.records.get(hash)
    }

    pub fn insert(&mut self, hash: Hash, record: ArchiveRecord) {
        for tag in &record.tags {
            let entry = self.by_tag.entry(tag.clone()).or_default();
            entry.insert(hash);
        }

        self.by_created.insert(record.created, hash);
        self.records.insert(hash, record);
    }

    pub fn remove(&mut self, hash: &Hash) -> Result<()> {
        let record = self
            .records
            .remove(hash)
            .ok_or_else(|| Error::ItemNotFound(hash.to_string()))?;

        self.by_created.remove(&record.created);

        for tag in record.tags {
            let hashes = self
                .by_tag
                .get_mut(&tag)
                .ok_or_else(|| Error::ItemNotFound(tag.clone()))?;
            hashes.remove(hash);
        }

        Ok(())
    }

    pub fn iter_by_created(&self) -> impl Iterator<Item = (&Hash, &ArchiveRecord)> {
        self.by_created.values().map(|hash| {
            let record = self.records.get(hash).unwrap();
            (hash, record)
        })
    }
}
