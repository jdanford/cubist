mod archives;
mod backup;
mod cleanup;
mod delete;
mod restore;

mod args;
mod parse;
mod storage;

use std::{
    fmt::Display,
    io::{stdout, Write},
    process::exit,
};

use clap::{
    builder::{styling::AnsiColor, Styles},
    Parser, Subcommand,
};
use concolor_clap::{color_choice, ColorChoice};
use env_logger::WriteStyle;
use log::{error, LevelFilter};

use crate::{error::Result, logger, stats::FinalizedCommandStats};

use self::args::{
    ArchivesArgs, BackupArgs, CleanupArgs, DeleteArgs, GlobalArgs, LoggerArgs, RestoreArgs,
};

/// Fast deduplicated backups on top of S3
#[derive(Parser, Debug)]
#[command(
    version,
    about,
    long_about = None,
    propagate_version = true,
    styles = cli_styles(),
    color = color_choice(),
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

    /// Clean up orphaned blocks and archives
    Cleanup(CleanupArgs),
}

impl Command {
    fn global(&self) -> &GlobalArgs {
        match self {
            Command::Backup(args) => &args.global,
            Command::Restore(args) => &args.global,
            Command::Delete(args) => &args.global,
            Command::Archives(args) => &args.global,
            Command::Cleanup(args) => &args.global,
        }
    }
}

pub async fn main() {
    let cli = Cli::parse();
    let global = cli.command.global();

    let level = log_level_from_args(&global.logger);
    let style = write_style_from_color_choice(global.logger.color);
    logger::init(level, style);

    let result = match cli.command {
        Command::Backup(args) => backup::main(args).await,
        Command::Restore(args) => restore::main(args).await,
        Command::Delete(args) => delete::main(args).await,
        Command::Archives(args) => archives::main(args).await,
        Command::Cleanup(args) => cleanup::main(args).await,
    };

    if let Err(err) = result {
        error!("{err}");
        exit(1);
    }
}

fn log_level_from_args(args: &LoggerArgs) -> LevelFilter {
    let base_verbosity: i8 = args.verbose.try_into().unwrap();
    let quietness: i8 = args.quiet.try_into().unwrap();
    let verbosity = base_verbosity - quietness;
    match verbosity {
        -2 => LevelFilter::Error,
        -1 => LevelFilter::Warn,
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    }
}

fn write_style_from_color_choice(color: ColorChoice) -> WriteStyle {
    match color {
        ColorChoice::Auto => WriteStyle::Auto,
        ColorChoice::Always => WriteStyle::Always,
        ColorChoice::Never => WriteStyle::Never,
    }
}

fn cli_styles() -> Styles {
    Styles::styled()
        .usage(AnsiColor::BrightCyan.on_default().underline())
        .header(AnsiColor::BrightCyan.on_default().underline())
        .literal(AnsiColor::BrightBlue.on_default())
        .placeholder(AnsiColor::BrightMagenta.on_default())
}

fn print_stat<T: Display>(name: &str, value: T) {
    let style = AnsiColor::Cyan.on_default();
    println!("{style}{name}:{style:#} {value}");
}

fn print_stats_json(stats: &FinalizedCommandStats) -> Result<()> {
    serde_json::to_writer_pretty(stdout(), stats)?;
    writeln!(stdout())?;
    Ok(())
}
