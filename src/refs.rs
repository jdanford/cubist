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
pub struct RefCounts {
    map: HashMap<Hash, u64>,
}

impl RefCounts {
    pub fn new() -> Self {
        RefCounts {
            map: HashMap::new(),
        }
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

    pub fn increment(&mut self, hash: &Hash) {
        self.add_count(hash, 1);
    }

    pub fn add(&mut self, rhs: &Self) {
        for (hash, &count) in &rhs.map {
            self.add_count(hash, count);
        }
    }

    pub fn sub(&mut self, rhs: &Self) -> Result<HashSet<Hash>> {
        let mut removed = HashSet::new();

        for (&hash, &rhs_count) in &rhs.map {
            let lhs_count = self.map.get(&hash).copied().unwrap_or(0);
            match lhs_count.cmp(&rhs_count) {
                Ordering::Greater => {
                    let count = rhs_count - lhs_count;
                    self.map.insert(hash, count);
                }
                Ordering::Equal => {
                    self.map.remove(&hash);
                    removed.insert(hash);
                }
                Ordering::Less => {
                    return Err(Error::WrongRefCount {
                        hash,
                        actual: lhs_count,
                        expected: rhs_count,
                    });
                }
            }
        }

        Ok(removed)
    }
}
