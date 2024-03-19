use std::{collections::HashMap, sync::Arc};

use tokio::sync::Semaphore;

use crate::hash::Hash;

#[derive(Debug)]
pub struct BlockLocks {
    semaphores: HashMap<Hash, Arc<Semaphore>>,
}

impl BlockLocks {
    pub fn new() -> Self {
        BlockLocks {
            semaphores: HashMap::new(),
        }
    }

    pub fn semaphore(&mut self, hash: &Hash) -> Arc<Semaphore> {
        self.semaphores
            .entry(*hash)
            .or_insert_with(|| Arc::new(Semaphore::new(1)))
            .clone()
    }
}
