use std::io::{self, Write};

use env_logger::fmt::Formatter;
use log::{Level, Record};

pub fn init(verbose: u8, quiet: u8) {
    let level = log_level_from_args(verbose, quiet);
    env_logger::Builder::new()
        .format(format)
        .filter_level(level)
        .init();
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

fn format(f: &mut Formatter, record: &Record) -> io::Result<()> {
    let level = record.level();
    let prefix_style = f.default_level_style(level);
    let prefix = level_prefix(level);
    writeln!(f, "{prefix_style}{prefix}{prefix_style:#}{}", record.args())
}

fn level_prefix(level: Level) -> &'static str {
    match level {
        Level::Debug | Level::Trace | Level::Info => "",
        Level::Warn => "warning: ",
        Level::Error => "error: ",
    }
}
