use std::path::Path;

use chrono::{DateTime, Local, Utc};
use humansize::{ToF64, Unsigned, DECIMAL};

pub fn format_path(path: &Path) -> String {
    let s: &str = &path.to_string_lossy();
    snailquote::escape(s).to_string()
}

pub fn format_size<T: ToF64 + Unsigned>(input: T) -> String {
    humansize::format_size(input, DECIMAL)
}

pub fn format_time(time: &DateTime<Utc>) -> String {
    time.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}
