mod blocks;
mod cli;
mod files;

pub use self::cli::main;

use std::{collections::HashMap, path::PathBuf};

use crate::{archive::Archive, file::Metadata, hash::Hash, storage::BoxedStorage};

use self::files::LocalBlock;

pub struct RestoreArgs {
    pub storage: BoxedStorage,
    pub max_concurrency: usize,
    pub output_path: PathBuf,
    pub archive: Archive,
}

pub struct RestoreState {
    pub local_blocks: HashMap<Hash, LocalBlock>,
}

pub struct PendingDownload {
    pub metadata: Metadata,
    pub hash: Option<Hash>,
    pub path: PathBuf,
}
