use std::{borrow::Cow, path::Path};

use chrono::{DateTime, Local, Utc};
use humansize::{ToF64, Unsigned, DECIMAL};

pub fn format_path(path: &Path) -> String {
    let path_str = path.to_string_lossy();
    let escaped_path = snailquote::escape(&path_str);
    if let Cow::Owned(owned_path) = escaped_path {
        owned_path
    } else {
        path_str.to_string()
    }
}

pub fn format_size<T: ToF64 + Unsigned>(input: T) -> String {
    humansize::format_size(input, DECIMAL)
}

pub fn format_time(time: &DateTime<Utc>) -> String {
    time.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}
