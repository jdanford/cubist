mod blocks;
mod cli;
mod files;

use std::path::PathBuf;

use crate::archive::Archive;
use crate::storage::BoxedStorage;

pub use self::cli::main;

pub use self::blocks::UploadTree;

pub struct BackupArgs {
    pub storage: BoxedStorage,
    pub compression_level: u32,
    pub target_block_size: u32,
    pub max_concurrency: usize,
    pub paths: Vec<PathBuf>,
}

struct BackupState {
    archive: Archive,
}

struct PendingUpload {
    local_path: PathBuf,
    archive_path: PathBuf,
}
