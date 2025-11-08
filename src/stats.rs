use std::{ops::Deref, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer, ser::SerializeMap};

use crate::{archive::Archive, block::Block, entity::Entity};

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

    pub fn finalize(self, storage_stats: StorageStats) -> FinalizedCommandStats {
        FinalizedCommandStats {
            command: self,
            storage: storage_stats,
            end_time: Utc::now(),
        }
    }
}

pub trait EntityStats<E: Entity> {
    fn add_entities_deleted(&mut self, count: u64);
}

impl EntityStats<Archive> for CommandStats {
    fn add_entities_deleted(&mut self, count: u64) {
        self.archives_deleted += count;
    }
}

impl EntityStats<Block> for CommandStats {
    fn add_entities_deleted(&mut self, count: u64) {
        self.blocks_deleted += count;
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Get,
    Put,
    Delete,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestInfo {
    #[serde(rename = "type")]
    pub kind: RequestKind,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct StorageStats {
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    pub requests: Vec<RequestInfo>,
}

impl StorageStats {
    pub fn new() -> Self {
        StorageStats {
            bytes_downloaded: 0,
            bytes_uploaded: 0,
            requests: Vec::new(),
        }
    }

    pub fn add_get(&mut self, start_time: DateTime<Utc>, end_time: DateTime<Utc>, bytes: u32) {
        let stats = RequestInfo {
            kind: RequestKind::Get,
            start_time,
            end_time,
            bytes: Some(bytes),
        };

        self.bytes_downloaded += u64::from(bytes);
        self.requests.push(stats);
    }

    pub fn add_put(&mut self, start_time: DateTime<Utc>, end_time: DateTime<Utc>, bytes: u32) {
        let stats = RequestInfo {
            kind: RequestKind::Put,
            start_time,
            end_time,
            bytes: Some(bytes),
        };

        self.bytes_uploaded += u64::from(bytes);
        self.requests.push(stats);
    }

    pub fn add_delete(&mut self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) {
        let stats = RequestInfo {
            kind: RequestKind::Delete,
            start_time,
            end_time,
            bytes: None,
        };

        self.requests.push(stats);
    }
}

#[derive(Debug, Clone)]
pub struct FinalizedCommandStats {
    command: CommandStats,
    pub storage: StorageStats,
    pub end_time: DateTime<Utc>,
}

impl FinalizedCommandStats {
    pub fn elapsed_time(&self) -> Duration {
        let delta = self.end_time - self.command.start_time;
        let ms = delta.num_milliseconds().try_into().unwrap();
        Duration::from_millis(ms)
    }

    pub fn metadata_bytes_downloaded(&self) -> u64 {
        self.storage.bytes_downloaded - self.command.content_bytes_downloaded
    }

    pub fn metadata_bytes_uploaded(&self) -> u64 {
        self.storage.bytes_uploaded - self.command.content_bytes_uploaded
    }

    pub fn download_speed(&self) -> u64 {
        bits_per_second(self.storage.bytes_downloaded, self.elapsed_time())
    }

    pub fn upload_speed(&self) -> u64 {
        bits_per_second(self.storage.bytes_uploaded, self.elapsed_time())
    }
}

impl Deref for FinalizedCommandStats {
    type Target = CommandStats;

    fn deref(&self) -> &Self::Target {
        &self.command
    }
}

impl Serialize for FinalizedCommandStats {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("start_time", &self.start_time)?;
        map.serialize_entry("end_time", &self.end_time)?;
        map.serialize_entry("content_bytes_downloaded", &self.content_bytes_downloaded)?;
        map.serialize_entry("content_bytes_uploaded", &self.content_bytes_uploaded)?;
        map.serialize_entry(
            "metadata_bytes_downloaded",
            &self.metadata_bytes_downloaded(),
        )?;
        map.serialize_entry("metadata_bytes_uploaded", &self.metadata_bytes_uploaded())?;
        map.serialize_entry("bytes_read", &self.bytes_read)?;
        map.serialize_entry("bytes_written", &self.bytes_written)?;
        map.serialize_entry("bytes_deleted", &self.bytes_deleted)?;
        map.serialize_entry("files_read", &self.files_read)?;
        map.serialize_entry("files_created", &self.files_created)?;
        map.serialize_entry("archives_deleted", &self.archives_deleted)?;
        map.serialize_entry("blocks_downloaded", &self.blocks_downloaded)?;
        map.serialize_entry("blocks_uploaded", &self.blocks_uploaded)?;
        map.serialize_entry("blocks_deleted", &self.blocks_deleted)?;
        map.serialize_entry("blocks_referenced", &self.blocks_referenced)?;
        map.serialize_entry("requests", &self.storage.requests)?;

        map.end()
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn bits_per_second(bytes: u64, elapsed_time: Duration) -> u64 {
    (bytes as f64 * 8.0 / elapsed_time.as_secs_f64()) as u64
}
