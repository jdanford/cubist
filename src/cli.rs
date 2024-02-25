use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::block::{DEFAULT_COMPRESSION_LEVEL, DEFAULT_TARGET_BLOCK_SIZE};

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

        #[arg(short, long)]
        bucket: String,

        paths: Vec<PathBuf>,
    },
    Restore {
        #[arg(short, long)]
        bucket: String,

        path: PathBuf,
    },
}
