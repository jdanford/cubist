use std::{collections::HashMap, sync::Arc};

use tokio::sync::Semaphore;

use crate::hash::Hash;

#[derive(Debug)]
pub struct BlockLocks {
    locks: HashMap<Hash, Arc<Semaphore>>,
}

impl BlockLocks {
    pub fn new() -> Self {
        BlockLocks {
            locks: HashMap::new(),
        }
    }

    pub fn lock(&mut self, hash: &Hash) -> Arc<Semaphore> {
        self.locks
            .entry(*hash)
            .or_insert_with(|| Arc::new(Semaphore::new(1)))
            .clone()
    }
}
