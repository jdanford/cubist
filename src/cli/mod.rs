mod archives;
mod backup;
mod delete;
mod restore;

mod arc;
mod locks;
mod ops;
mod storage;

use std::{path::PathBuf, time::Duration};

use clap::{
    builder::{styling::AnsiColor, Styles},
    ArgAction, Args, Parser, Subcommand,
};
use humantime::parse_duration;
use log::error;

use crate::{file::WalkOrder, logger, storage::StorageUrl};

const DEFAULT_COMPRESSION_LEVEL: u8 = 3;
const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: u32 = 64;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None, propagate_version = true, styles = cli_styles())]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
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
struct BackupArgs {
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    #[arg(short = 'l', long, value_name = "LEVEL", default_value_t = DEFAULT_COMPRESSION_LEVEL)]
    pub compression_level: u8,

    #[arg(short = 'b', long, value_name = "SIZE", default_value_t = DEFAULT_TARGET_BLOCK_SIZE)]
    pub target_block_size: u32,

    #[arg(short, long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub jobs: u32,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
struct RestoreArgs {
    pub archive: String,

    pub paths: Vec<PathBuf>,

    #[arg(long, default_value_t = WalkOrder::DepthFirst)]
    pub order: WalkOrder,

    #[arg(short, long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub jobs: u32,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
struct DeleteArgs {
    #[arg(required = true)]
    pub archives: Vec<String>,

    #[arg(short, long, value_name = "N", default_value_t = DEFAULT_MAX_CONCURRENCY)]
    pub jobs: u32,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
struct ArchivesArgs {
    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
struct GlobalArgs {
    #[arg(short, long)]
    pub storage: Option<StorageUrl>,

    #[arg(long, value_parser = parse_duration)]
    pub latency: Option<Duration>,

    #[arg(long, default_value_t = false)]
    pub stats: bool,

    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub verbose: u8,

    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub quiet: u8,
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

fn cli_styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::BrightMagenta.on_default())
        .usage(AnsiColor::BrightMagenta.on_default())
        .literal(AnsiColor::BrightBlue.on_default())
        .placeholder(AnsiColor::BrightCyan.on_default())
}
