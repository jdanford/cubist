use std::{ops::Deref, time::Duration};

use chrono::{DateTime, Utc};
use humansize::{ToF64, Unsigned, DECIMAL};

#[derive(Debug)]
pub struct CoreStats {
    pub start_time: DateTime<Utc>,
    pub content_bytes_downloaded: u64,
    pub content_bytes_uploaded: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_deleted: u64,
    pub files_read: u64,
    pub files_created: u64,
    pub blocks_downloaded: u64,
    pub blocks_uploaded: u64,
    pub blocks_deleted: u64,
    pub blocks_referenced: u64,
}

impl CoreStats {
    pub fn new() -> Self {
        CoreStats {
            start_time: Utc::now(),
            content_bytes_downloaded: 0,
            content_bytes_uploaded: 0,
            bytes_read: 0,
            bytes_written: 0,
            bytes_deleted: 0,
            files_read: 0,
            files_created: 0,
            blocks_downloaded: 0,
            blocks_uploaded: 0,
            blocks_deleted: 0,
            blocks_referenced: 0,
        }
    }

    pub fn finalize(self, storage: &StorageStats) -> FinalizedStats {
        let end_time = Utc::now();
        let storage = storage.to_owned();
        FinalizedStats {
            core: self,
            storage,
            end_time,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageStats {
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    pub get_requests: u64,
    pub put_requests: u64,
    pub delete_requests: u64,
}

impl StorageStats {
    pub fn new() -> Self {
        StorageStats {
            bytes_downloaded: 0,
            bytes_uploaded: 0,
            get_requests: 0,
            put_requests: 0,
            delete_requests: 0,
        }
    }
}

#[derive(Debug)]
pub struct FinalizedStats {
    pub core: CoreStats,
    pub storage: StorageStats,
    pub end_time: DateTime<Utc>,
}

impl FinalizedStats {
    pub fn elapsed_time(&self) -> Duration {
        let delta = self.end_time - self.core.start_time;
        let ms = delta.num_milliseconds().try_into().unwrap();
        Duration::from_millis(ms)
    }

    pub fn metadata_bytes_downloaded(&self) -> u64 {
        self.storage.bytes_downloaded - self.core.content_bytes_downloaded
    }

    pub fn metadata_bytes_uploaded(&self) -> u64 {
        self.storage.bytes_uploaded - self.core.content_bytes_uploaded
    }
}

impl Deref for FinalizedStats {
    type Target = CoreStats;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub fn format_size<T: ToF64 + Unsigned>(input: T) -> String {
    humansize::format_size(input, DECIMAL)
}
