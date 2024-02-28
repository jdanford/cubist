use std::path::PathBuf;

use clap::{Parser, Subcommand};

const DEFAULT_COMPRESSION_LEVEL: u32 = 3;
const DEFAULT_TARGET_BLOCK_SIZE: u32 = 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 64;
const DEFAULT_BUCKET: &str = "cubist";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Backup {
        #[arg(long, default_value_t = DEFAULT_COMPRESSION_LEVEL)]
        compression_level: u32,

        #[arg(long, default_value_t = DEFAULT_TARGET_BLOCK_SIZE)]
        target_block_size: u32,

        #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY)]
        max_concurrency: usize,

        #[arg(short, long, default_value = DEFAULT_BUCKET)]
        bucket: String,

        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
    Restore {
        #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY)]
        max_concurrency: usize,

        #[arg(short, long, default_value = DEFAULT_BUCKET)]
        bucket: String,

        #[arg(required = true)]
        path: PathBuf,
    },
}
