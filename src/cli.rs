use std::{path::PathBuf, time::Duration};

use clap::{ArgAction, Args, Parser, Subcommand};

use crate::{
    logger,
    storage::{BoxedStorage, LocalStorage, S3Storage},
};

const DEFAULT_COMPRESSION_LEVEL: u8 = 3;
const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: u32 = 64;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Backup(#[command(flatten)] BackupArgs),
    Restore(#[command(flatten)] RestoreArgs),
    Delete(#[command(flatten)] DeleteArgs),
}

#[derive(Args, Debug)]
pub struct BackupArgs {
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    #[arg(long, value_name = "LEVEL", default_value_t = DEFAULT_COMPRESSION_LEVEL)]
    pub compression_level: u8,

    #[arg(long, value_name = "SIZE", default_value_t = DEFAULT_TARGET_BLOCK_SIZE)]
    pub target_block_size: u32,

    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub max_concurrency: u32,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    pub archive_name: String,

    pub paths: Vec<PathBuf>,

    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub max_concurrency: u32,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    pub archive_name: String,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
#[clap(group(clap::ArgGroup::new("storage").required(true)))]
pub struct StorageArgs {
    #[arg(long, group = "storage")]
    pub bucket: Option<String>,

    #[arg(
        long,
        value_name = "STORAGE_PATH",
        group = "storage",
        group = "storage-local"
    )]
    pub local: Option<PathBuf>,

    #[arg(long, value_parser = humantime::parse_duration, requires = "storage-local")]
    pub latency: Option<Duration>,
}

#[derive(Args, Debug)]
pub struct LoggerArgs {
    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub verbose: u8,

    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub quiet: u8,
}

pub async fn create_storage(args: StorageArgs) -> BoxedStorage {
    match args {
        StorageArgs {
            bucket: Some(bucket),
            ..
        } => Box::new(S3Storage::new(bucket).await),
        StorageArgs {
            local: Some(path),
            latency,
            ..
        } => Box::new(LocalStorage::new(path, latency)),
        _ => unreachable!(),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn init_logger(args: LoggerArgs) {
    let level = log_level_from_args(args.verbose, args.quiet);
    logger::init(level);
}

fn log_level_from_args(verbose: u8, quiet: u8) -> log::LevelFilter {
    let base_verbosity: i8 = verbose.try_into().unwrap();
    let quiet_verbosity: i8 = quiet.try_into().unwrap();
    let verbosity = base_verbosity - quiet_verbosity;
    match verbosity {
        -2 => log::LevelFilter::Error,
        -1 => log::LevelFilter::Warn,
        0 => log::LevelFilter::Info,
        1 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    }
}
