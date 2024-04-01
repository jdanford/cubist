use std::io::{self, Write};

use env_logger::{fmt::Formatter, WriteStyle};
use log::{Level, LevelFilter, Record};

pub fn init(level: LevelFilter, style: WriteStyle) {
    env_logger::Builder::new()
        .format(format)
        .filter_level(level)
        .write_style(style)
        .init();
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
