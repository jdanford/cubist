use std::{ops::{Deref, DerefMut}, time::SystemTime};

use crate::file::FileTree;

pub struct Archive {
    pub timestamp: i64,
    tree: FileTree,
}

impl Archive {
    pub fn new() -> Self {
        Archive {
            timestamp: now_timestamp(),
            tree: FileTree::new(),
        }
    }
}

impl Deref for Archive {
    type Target = FileTree;

    fn deref(&self) -> &Self::Target {
        &self.tree
    }
}

impl DerefMut for Archive {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tree
    }
}

const PANIC_SYSTEM_TIME: &str = "system time out of range";

pub fn unix_timestamp(time: SystemTime) -> i64 {
    let duration = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect(PANIC_SYSTEM_TIME);
    duration.as_secs().try_into().expect(PANIC_SYSTEM_TIME)
}

fn now_timestamp() -> i64 {
    unix_timestamp(SystemTime::now())
}
