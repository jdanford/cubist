use std::{ops::Deref, time::Duration};

use chrono::{DateTime, Utc};

use crate::storage::BoxedStorage;

#[derive(Debug, Clone)]
pub struct CommandStats {
    pub start_time: DateTime<Utc>,
    pub content_bytes_downloaded: u64,
    pub content_bytes_uploaded: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_deleted: u64,
    pub files_read: u64,
    pub files_created: u64,
    pub archives_deleted: u64,
    pub blocks_downloaded: u64,
    pub blocks_uploaded: u64,
    pub blocks_deleted: u64,
    pub blocks_referenced: u64,
}

impl CommandStats {
    pub fn new() -> Self {
        CommandStats {
            start_time: Utc::now(),
            content_bytes_downloaded: 0,
            content_bytes_uploaded: 0,
            bytes_read: 0,
            bytes_written: 0,
            bytes_deleted: 0,
            files_read: 0,
            files_created: 0,
            archives_deleted: 0,
            blocks_downloaded: 0,
            blocks_uploaded: 0,
            blocks_deleted: 0,
            blocks_referenced: 0,
        }
    }

    pub fn finalize(self, storage: BoxedStorage) -> FinalizedCommandStats {
        FinalizedCommandStats {
            command: self,
            storage,
            end_time: Utc::now(),
        }
    }
}

#[derive(Debug)]
pub struct FinalizedCommandStats {
    command: CommandStats,
    pub storage: BoxedStorage,
    pub end_time: DateTime<Utc>,
}

impl FinalizedCommandStats {
    pub fn elapsed_time(&self) -> Duration {
        let delta = self.end_time - self.command.start_time;
        let ms = delta.num_milliseconds().try_into().unwrap();
        Duration::from_millis(ms)
    }

    pub fn metadata_bytes_downloaded(&self) -> u64 {
        self.storage.stats().bytes_downloaded - self.command.content_bytes_downloaded
    }

    pub fn metadata_bytes_uploaded(&self) -> u64 {
        self.storage.stats().bytes_uploaded - self.command.content_bytes_uploaded
    }
}

impl Deref for FinalizedCommandStats {
    type Target = CommandStats;

    fn deref(&self) -> &Self::Target {
        &self.command
    }
}
