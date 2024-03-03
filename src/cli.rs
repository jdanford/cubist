use std::{path::PathBuf, time::Duration};

use clap::{ArgAction, Args, Parser, Subcommand};

use crate::{
    logger,
    storage::{BoxedStorage, LocalStorage, S3Storage},
};

const DEFAULT_COMPRESSION_LEVEL: u32 = 3;
const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 64;

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
}

#[derive(Args, Debug)]
pub struct BackupArgs {
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    #[arg(long, default_value_t = DEFAULT_COMPRESSION_LEVEL, value_name = "LEVEL")]
    pub compression_level: u32,

    #[arg(long, default_value_t = DEFAULT_TARGET_BLOCK_SIZE, value_name = "SIZE")]
    pub target_block_size: u32,

    #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY, value_name = "N")]
    pub max_concurrency: usize,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    pub path: PathBuf,

    #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY, value_name = "N")]
    pub max_concurrency: usize,

    #[command(flatten)]
    pub storage: StorageArgs,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct StorageArgs {
    #[arg(long, group = "storage")]
    pub bucket: Option<String>,

    #[arg(long, value_name = "PATH", group = "storage", group = "storage-local")]
    pub local: Option<PathBuf>,

    #[arg(long, value_parser = parse_duration_ms, requires = "storage-local")]
    pub latency: Option<Duration>,
}

#[derive(Args, Debug)]
pub struct LoggerArgs {
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,
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
    let level = log_level_from_verbose(args.verbose);
    logger::init(level);
}

fn log_level_from_verbose(n: u8) -> log::LevelFilter {
    match n {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    }
}

fn parse_duration_ms(s: &str) -> Result<Duration, String> {
    let ms = s.parse().map_err(|_| format!("`{s}` is out of range"))?;
    Ok(Duration::from_millis(ms))
}
