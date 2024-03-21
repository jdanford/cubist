mod archives;
mod backup;
mod delete;
mod restore;

mod arc;
mod locks;
mod logger;
mod ops;
mod parser;
mod storage;

use std::{ops::RangeInclusive, path::PathBuf, time::Duration};

use clap::{
    builder::{styling::AnsiColor, Styles},
    ArgAction, Args, Parser, Subcommand,
};
use humantime::parse_duration;
use log::error;

use crate::{file::WalkOrder, storage::StorageUrl};

use self::parser::parse_range_inclusive;

#[derive(Parser, Debug)]
#[command(
    version,
    about,
    long_about = None,
    propagate_version = true,
    styles = cli_styles(),
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Back up files to an archive
    Backup(BackupArgs),

    /// Restore files from an archive
    Restore(RestoreArgs),

    /// Delete one or more archives
    Delete(DeleteArgs),

    /// List archives
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
    /// Files to back up
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    /// Compression level (1-19)
    #[arg(
        short = 'l',
        long,
        value_name = "NUM",
        default_value_t = DEFAULT_COMPRESSION_LEVEL,
        value_parser = parse_compression_level,
    )]
    pub compression_level: u8,

    /// Target size for blocks
    #[arg(
        short = 'b',
        long,
        value_name = "NUM",
        default_value_t = DEFAULT_TARGET_BLOCK_SIZE,
        value_parser = parse_block_size,
    )]
    pub target_block_size: u32,

    /// Number of background tasks to use
    #[arg(
        short = 'j',
        long,
        value_name = "NUM",
        default_value_t = DEFAULT_TASK_COUNT,
        value_parser = parse_task_count,
    )]
    pub tasks: usize,

    /// Show operations that would be performed without actually doing them
    #[arg(short = 'n', long, default_value_t = false)]
    pub dry_run: bool,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
struct RestoreArgs {
    /// Archive to restore from
    pub archive: String,

    /// Files to restore (or all files if empty)
    pub paths: Vec<PathBuf>,

    /// Archive traversal order
    #[arg(long, default_value_t = WalkOrder::DepthFirst)]
    pub order: WalkOrder,

    /// Number of background tasks to use
    #[arg(
        short = 'j',
        long,
        value_name = "NUM",
        default_value_t = DEFAULT_TASK_COUNT,
        value_parser = parse_task_count,
    )]
    pub tasks: usize,

    /// Show operations that would be performed without actually doing them
    #[arg(short = 'n', long, default_value_t = false)]
    pub dry_run: bool,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
struct DeleteArgs {
    /// Archive(s) to delete
    #[arg(required = true)]
    pub archives: Vec<String>,

    /// Number of background tasks to use
    #[arg(
        short = 'j',
        long,
        value_name = "NUM",
        default_value_t = DEFAULT_TASK_COUNT,
        value_parser = parse_task_count,
    )]
    pub tasks: usize,

    /// Show operations that would be performed without actually doing them
    #[arg(short = 'n', long, default_value_t = false)]
    pub dry_run: bool,

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
    /// Storage backend (e.g. 's3://<bucket>' or 'file://<path>')
    #[arg(short, long, value_name = "URL")]
    pub storage: Option<StorageUrl>,

    /// Add latency when using local storage
    #[arg(short = 'L', long, value_parser = parse_duration)]
    pub latency: Option<Duration>,

    /// Print stats after completion
    #[arg(long, default_value_t = false)]
    pub stats: bool,

    /// Print more output
    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub verbose: u8,

    /// Print less output
    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub quiet: u8,
}

pub async fn main() {
    let cli = Cli::parse();
    let global = cli.command.global();
    logger::init(global.verbose, global.quiet);

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

fn cli_styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::BrightMagenta.on_default())
        .usage(AnsiColor::BrightMagenta.on_default())
        .literal(AnsiColor::BrightBlue.on_default())
        .placeholder(AnsiColor::BrightCyan.on_default())
}

const DEFAULT_COMPRESSION_LEVEL: u8 = 3;
const COMPRESSION_LEVEL_RANGE: RangeInclusive<u8> = 1..=19;

fn parse_compression_level(s: &str) -> Result<u8, String> {
    parse_range_inclusive(s, COMPRESSION_LEVEL_RANGE)
}

const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1 << 20;
const BLOCK_SIZE_RANGE: RangeInclusive<u32> = 1..=u32::MAX;

fn parse_block_size(s: &str) -> Result<u32, String> {
    parse_range_inclusive(s, BLOCK_SIZE_RANGE)
}

const DEFAULT_TASK_COUNT: usize = 64;
const TASK_COUNT_RANGE: RangeInclusive<usize> = 1..=1024;

fn parse_task_count(s: &str) -> Result<usize, String> {
    parse_range_inclusive(s, TASK_COUNT_RANGE)
}
