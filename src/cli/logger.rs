use std::io::{self, Write};

use concolor_clap::ColorChoice;
use env_logger::{fmt::Formatter, WriteStyle};
use log::{Level, LevelFilter, Record};

use super::args::LoggerArgs;

pub fn init(args: &LoggerArgs) {
    let level = log_level_from_args(args);
    let style = write_style_from_color_choice(args.color);
    env_logger::Builder::new()
        .format(format)
        .filter_level(level)
        .write_style(style)
        .init();
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

fn format(f: &mut Formatter, record: &Record) -> io::Result<()> {
    let args = record.args();
    let level = record.level();
    if let Some(prefix) = level_prefix(level) {
        let style = f.default_level_style(level);
        writeln!(f, "{style}{prefix}{style:#}{args}")
    } else {
        writeln!(f, "{args}")
    }
}

fn level_prefix(level: Level) -> Option<&'static str> {
    match level {
        Level::Debug | Level::Trace | Level::Info => None,
        Level::Warn => Some("warning: "),
        Level::Error => Some("error: "),
    }
}
