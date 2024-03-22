use std::io::{self, Write};

use concolor_clap::ColorChoice;
use env_logger::{fmt::Formatter, WriteStyle};
use log::{Level, Record};

use super::LoggerArgs;

pub fn init(args: &LoggerArgs) {
    let level = log_level_from_args(args);
    let style = write_style_from_color_choice(args.color);
    env_logger::Builder::new()
        .format(format)
        .filter_level(level)
        .write_style(style)
        .init();
}

fn log_level_from_args(args: &LoggerArgs) -> log::LevelFilter {
    let base_verbosity: i8 = args.verbose.try_into().unwrap();
    let quiet_verbosity: i8 = args.quiet.try_into().unwrap();
    let verbosity = base_verbosity - quiet_verbosity;
    match verbosity {
        -2 => log::LevelFilter::Error,
        -1 => log::LevelFilter::Warn,
        0 => log::LevelFilter::Info,
        1 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    }
}

fn format(f: &mut Formatter, record: &Record) -> io::Result<()> {
    let level = record.level();
    let prefix = level_prefix(level);
    let prefix_style = f.default_level_style(level);
    writeln!(f, "{prefix_style}{prefix}{prefix_style:#}{}", record.args())
}

fn level_prefix(level: Level) -> &'static str {
    match level {
        Level::Debug | Level::Trace | Level::Info => "",
        Level::Warn => "warning: ",
        Level::Error => "error: ",
    }
}

fn write_style_from_color_choice(color: ColorChoice) -> WriteStyle {
    match color {
        ColorChoice::Auto => WriteStyle::Auto,
        ColorChoice::Always => WriteStyle::Always,
        ColorChoice::Never => WriteStyle::Never,
    }
}