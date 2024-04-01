use std::{ops::RangeInclusive, path::PathBuf, time::Duration};

use clap::{ArgAction, Args};
use concolor_clap::ColorChoice;
use humantime::parse_duration;

use crate::{file::WalkOrder, hash::ShortHash, storage::StorageUrl};

use super::parse::{parse_range_inclusive, parse_short_hash};

const COMPRESSION_LEVEL_RANGE: RangeInclusive<u8> = 1..=19;
const DEFAULT_COMPRESSION_LEVEL: u8 = 3;

const BLOCK_SIZE_RANGE: RangeInclusive<u32> = 1..=u32::MAX;
const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1 << 20;

const TASK_COUNT_RANGE: RangeInclusive<usize> = 1..=1024;
const DEFAULT_TASK_COUNT: usize = 8;

fn parse_compression_level(s: &str) -> Result<u8, String> {
    parse_range_inclusive(s, COMPRESSION_LEVEL_RANGE)
}

fn parse_block_size(s: &str) -> Result<u32, String> {
    parse_range_inclusive(s, BLOCK_SIZE_RANGE)
}

fn parse_task_count(s: &str) -> Result<usize, String> {
    parse_range_inclusive(s, TASK_COUNT_RANGE)
}

#[derive(Args, Debug)]
pub struct BackupArgs {
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

    /// Undo all changes when finished
    #[arg(short = 't', long, default_value_t = false)]
    pub transient: bool,

    /// Show operations that would be performed without actually doing them
    #[arg(short = 'n', long, default_value_t = false)]
    pub dry_run: bool,

    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Archive to restore from
    #[arg(value_parser = parse_short_hash)]
    pub archive: ShortHash,

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
pub struct DeleteArgs {
    /// Archive(s) to delete
    #[arg(required = true, value_parser = parse_short_hash)]
    pub archives: Vec<ShortHash>,

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
pub struct ArchivesArgs {
    #[command(flatten)]
    pub global: GlobalArgs,
}

#[derive(Args, Debug)]
pub struct GlobalArgs {
    /// Storage backend (e.g. 's3://<bucket>' or 'file://<path>')
    #[arg(short, long, value_name = "URL")]
    pub storage: Option<StorageUrl>,

    /// Add latency when using local storage
    #[arg(short = 'L', long, value_parser = parse_duration)]
    pub latency: Option<Duration>,

    /// Print stats after completion
    #[arg(long, default_value_t = false)]
    pub stats: bool,

    #[command(flatten)]
    pub logger: LoggerArgs,
}

#[derive(Args, Debug)]
pub struct LoggerArgs {
    /// When to use color in output
    #[arg(short, long, default_value_t = ColorChoice::Auto)]
    pub color: ColorChoice,

    /// Print more output
    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub verbose: u8,

    /// Print less output
    #[arg(short, long, action = ArgAction::Count, group = "verbosity")]
    pub quiet: u8,
}
