mod range;
#[cfg(test)]
mod tests;

use std::cmp::Ordering;

use crate::error::{Error, Result};

use self::range::find_prefix_range;

pub fn longest_common_prefix<S: AsRef<str>>(strings: &[S]) -> Option<&str> {
    let (first_generic, rest) = strings.split_first()?;
    let first = first_generic.as_ref();
    let first_bytes = first.as_bytes();
    let mut min_len = first.len();

    for s in rest {
        let len = s
            .as_ref()
            .as_bytes()
            .iter()
            .zip(first_bytes)
            .take_while(|&(a, b)| a == b)
            .count();
        min_len = min_len.min(len);
    }

    if min_len == 0 {
        None
    } else {
        Some(&first[..min_len])
    }
}

pub fn find_one_by_prefix<'a, S: AsRef<str>>(strings: &'a [S], prefix: &str) -> Result<&'a str> {
    if let Some(range) = find_prefix_range(strings, prefix, &cmp_prefix) {
        let i = *range.start();
        let j = *range.end();
        if i == j {
            Ok(strings[i].as_ref())
        } else {
            Err(Error::MultipleItemsForPrefix(prefix.to_owned()))
        }
    } else {
        Err(Error::NoItemForPrefix(prefix.to_owned()))
    }
}

fn cmp_prefix(string: &str, prefix: &str) -> Ordering {
    let len = prefix.len();
    let short_string = &string[..len];
    short_string.cmp(prefix)
}
