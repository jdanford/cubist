mod archives;
mod backup;
mod delete;
mod restore;

mod args;
mod format;
mod logger;
mod parse;
mod storage;

use std::fmt::Display;

use clap::{
    builder::{styling::AnsiColor, Styles},
    Parser, Subcommand,
};
use concolor_clap::color_choice;
use log::error;

use self::args::{ArchivesArgs, BackupArgs, DeleteArgs, GlobalArgs, RestoreArgs};

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

pub async fn main() {
    let cli = Cli::parse();
    let global = cli.command.global();
    logger::init(&global.logger);

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
        .usage(AnsiColor::BrightCyan.on_default().underline())
        .header(AnsiColor::BrightCyan.on_default().underline())
        .literal(AnsiColor::BrightBlue.on_default())
        .placeholder(AnsiColor::BrightMagenta.on_default())
}

pub fn print_stat<T: Display>(name: &str, value: T) {
    let style = AnsiColor::Cyan.on_default();
    println!("{style}{name}:{style:#} {value}");
}
