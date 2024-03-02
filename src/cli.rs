use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::{
    hash::Hash,
    logger,
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
pub struct StorageArgs {
    #[arg(long)]
    pub bucket: Option<String>,

    #[arg(long)]
    pub local: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct LoggerArgs {
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Args, Debug)]
pub struct BackupArgs {
    #[arg(long, default_value_t = DEFAULT_COMPRESSION_LEVEL, value_name = "LEVEL")]
    pub compression_level: u32,

    #[arg(long, default_value_t = DEFAULT_TARGET_BLOCK_SIZE, value_name = "SIZE")]
    pub target_block_size: u32,

    #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY, value_name = "N")]
    pub max_concurrency: usize,

    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY, value_name = "N")]
    pub max_concurrency: usize,

    pub path: PathBuf,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    pub hash: Hash,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Backup(#[command(flatten)] BackupArgs),
    Restore(#[command(flatten)] RestoreArgs),
    Inspect(#[command(flatten)] InspectArgs),
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

#[allow(clippy::needless_pass_by_value)]
pub fn init_logger(args: LoggerArgs) {
    let level = match args.verbose {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    env_logger::Builder::new()
        .format(logger::format)
        .filter_level(level)
        .try_init()
        .unwrap();
}
