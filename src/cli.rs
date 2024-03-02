use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::{
    hash::Hash,
    storage::{BoxedStorage, CloudStorage, LocalStorage},
};

const DEFAULT_COMPRESSION_LEVEL: u32 = 3;
const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 64;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Args, Debug)]
#[group(required = true, multiple = false)]
pub struct StorageArgs {
    #[arg(long)]
    pub bucket: Option<String>,

    #[arg(long)]
    pub local: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Backup {
        #[command(flatten)]
        storage_args: StorageArgs,

        #[arg(long, default_value_t = DEFAULT_COMPRESSION_LEVEL, value_name = "LEVEL")]
        compression_level: u32,

        #[arg(long, default_value_t = DEFAULT_TARGET_BLOCK_SIZE, value_name = "SIZE")]
        target_block_size: u32,

        #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY, value_name = "N")]
        max_concurrency: usize,

        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
    Restore {
        #[command(flatten)]
        storage_args: StorageArgs,

        #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY, value_name = "N")]
        max_concurrency: usize,

        path: PathBuf,
    },
    InspectBlock {
        #[command(flatten)]
        storage_args: StorageArgs,

        hash: Hash,
    },
}

pub async fn create_storage(args: StorageArgs) -> BoxedStorage {
    match args {
        StorageArgs {
            bucket: Some(bucket),
            ..
        } => Box::new(CloudStorage::new(bucket).await),
        StorageArgs {
            local: Some(path), ..
        } => Box::new(LocalStorage::new(path)),
        _ => unreachable!(),
    }
}
