use std::collections::{BTreeMap, HashMap, HashSet};

use blake3::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveRecord {
    pub created: DateTime<Utc>,
    pub tags: HashSet<String>,
}

#[derive(Debug)]
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

    pub fn from_records(records: HashMap<Hash, ArchiveRecord>) -> Self {
        let mut by_created = BTreeMap::new();
        let mut by_tag = HashMap::new();

        for (hash, record) in &records {
            insert_tags(&mut by_tag, *hash, &record.tags);
            by_created.insert(record.created, *hash);
        }

        ArchiveRecords {
            records,
            by_created,
            by_tag,
        }
    }

    #[allow(dead_code)]
    pub fn get(&self, hash: &Hash) -> Option<&ArchiveRecord> {
        self.records.get(hash)
    }

    pub fn insert(&mut self, hash: Hash, record: ArchiveRecord) {
        insert_tags(&mut self.by_tag, hash, &record.tags);
        self.by_created.insert(record.created, hash);
        self.records.insert(hash, record);
    }

    pub fn remove(&mut self, hash: &Hash) -> Result<()> {
        let record = self
            .records
            .remove(hash)
            .ok_or_else(|| Error::ItemNotFound(hash.to_string()))?;
        self.by_created.remove(&record.created);
        self.remove_tags(hash, &record.tags)?;
        Ok(())
    }

    fn remove_tags(&mut self, hash: &Hash, tags: &HashSet<String>) -> Result<()> {
        for tag in tags {
            let hashes =
                self.by_tag
                    .get_mut(tag.as_str())
                    .ok_or_else(|| Error::NoTagForArchive {
                        hash: hash.to_string(),
                        tag: tag.to_string(),
                    })?;
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

fn insert_tags(by_tag: &mut HashMap<String, HashSet<Hash>>, hash: Hash, tags: &HashSet<String>) {
    for tag in tags {
        let entry = by_tag.entry(tag.clone()).or_default();
        entry.insert(hash);
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
