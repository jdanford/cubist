use std::{iter, sync::Arc};

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{block::Block, hash::Hash};

#[derive(Debug)]
pub struct BlockLocks {
    locks: Vec<Arc<Mutex<()>>>,
}

impl BlockLocks {
    pub fn new(count: usize) -> Self {
        assert!(count > 0);
        assert!(u16::try_from(count).is_ok());

        BlockLocks {
            locks: iter::repeat_with(|| Arc::new(Mutex::new(())))
                .take(count)
                .collect(),
        }
    }

    fn size(&self) -> usize {
        self.locks.len()
    }

    fn lock(&self, hash: &Hash<Block>) -> Arc<Mutex<()>> {
        let index = block_hash_index(hash) % self.size();
        self.locks[index].clone()
    }

    pub async fn acquire(&self, hash: &Hash<Block>) -> OwnedMutexGuard<()> {
        self.lock(hash).lock_owned().await
    }
}

fn block_hash_index(hash: &Hash<Block>) -> usize {
    let bytes = *hash.as_bytes();
    let upper = bytes[30] as usize;
    let lower = bytes[31] as usize;
    upper << 8 | lower
}
