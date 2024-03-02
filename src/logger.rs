use std::io::{self, Write};

use env_logger::fmt::Formatter;
use log::{Level, Record};

pub fn format(f: &mut Formatter, record: &Record) -> io::Result<()> {
    let level = record.level();
    let prefix_style = f.default_level_style(level);
    let prefix = level_prefix(level);
    writeln!(f, "{prefix_style}{prefix}{prefix_style:#}{}", record.args())
}

fn level_prefix(level: Level) -> &'static str {
    match level {
        Level::Trace => "trace: ",
        Level::Debug => "debug: ",
        Level::Info => "",
        Level::Warn => "warning: ",
        Level::Error => "error: ",
    }
}
