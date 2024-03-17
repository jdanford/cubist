mod archives;
mod backup;
mod delete;
mod restore;

mod common;

use std::{path::PathBuf, time::Duration};

use clap::{ArgAction, Args, Parser, Subcommand};
use humantime::parse_duration;
use log::error;

use crate::logger;

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
    Backup(BackupArgs),
    Restore(RestoreArgs),
    Delete(DeleteArgs),
    Archives(ArchivesArgs),
}

impl Command {
    fn global(&self) -> &GlobalArgs {
        match self {
            Command::Backup(args) => &args.global,
            Command::Restore(args) => &args.global,
            Command::Delete(args) => &args.global,
            Command::Archives(args) => &args.global,
        }
    }
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
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    pub archive_name: String,

    pub paths: Vec<PathBuf>,

    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub max_concurrency: u32,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    pub archive_names: Vec<String>,

    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub max_concurrency: u32,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
pub struct ArchivesArgs {
    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
pub struct GlobalArgs {
    #[arg(short, long, default_value_t = false)]
    pub stats: bool,

    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub verbose: u8,

    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub quiet: u8,

    #[command(flatten)]
    pub storage: StorageArgs,
}

#[derive(Args, Debug)]
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

    #[arg(long, value_parser = parse_duration, requires = "storage-local")]
    pub latency: Option<Duration>,
}

pub async fn main() {
    let cli = Cli::parse();
    init_logger(cli.command.global());

    let result = match cli.command {
        Command::Backup(args) => backup::main(args).await,
        Command::Restore(args) => restore::main(args).await,
        Command::Delete(args) => delete::main(args).await,
        Command::Archives(args) => archives::main(args).await,
    };

    if let Err(err) = result {
        error!("{err}");
    }
}

fn init_logger(args: &GlobalArgs) {
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
