use std::path::Path;

use humansize::{ToF64, Unsigned, DECIMAL};

pub fn format_path(path: &Path) -> String {
    let s: &str = &path.to_string_lossy();
    snailquote::escape(s).to_string()
}

pub fn format_size<T: ToF64 + Unsigned>(input: T) -> String {
    humansize::format_size(input, DECIMAL)
}
