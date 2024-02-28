use std::os::unix::fs::MetadataExt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub inode: u64,
    pub mode: u32,
    pub group: u32,
    pub owner: u32,
    pub accessed: Option<DateTime<Utc>>,
    pub created: Option<DateTime<Utc>>,
    pub modified: Option<DateTime<Utc>>,
}

impl Metadata {
    pub fn from_native(native: &std::fs::Metadata) -> Self {
        Metadata {
            inode: native.ino(),
            mode: native.mode(),
            group: native.gid(),
            owner: native.uid(),
            accessed: native.accessed().ok().map(Into::into),
            created: native.created().ok().map(Into::into),
            modified: native.modified().ok().map(Into::into),
        }
    }
}
