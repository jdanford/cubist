use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    ops::RangeInclusive,
};

use crate::error::{Error, Result};

pub fn longest_common_prefix<'a>(strings: &'a [&'a str]) -> Option<&'a str> {
    if strings.is_empty() {
        return None;
    }

    let prefix = strings[0];
    let prefix_bytes = prefix.as_bytes();
    let mut min_len = prefix.len();

    for str in &strings[1..] {
        let len = str
            .as_bytes()
            .iter()
            .zip(prefix_bytes)
            .take_while(|&(a, b)| a == b)
            .count();
        min_len = min_len.min(len);
    }

    Some(&prefix[..min_len])
}

pub fn find_one_by_prefix<'a, S: AsRef<str>>(strings: &'a [S], prefix: &str) -> Result<&'a str> {
    if let Some(range) = find_prefix_range(strings, prefix) {
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

fn find_prefix_range<S: AsRef<str>>(values: &[S], prefix: &str) -> Option<RangeInclusive<usize>> {
    let start = find_prefix_range_start(values, prefix)?;
    let end = find_prefix_range_end(values, prefix)?;
    Some(start..=end)
}

#[allow(clippy::match_same_arms)]
fn find_prefix_range_start<S: AsRef<str>>(values: &[S], prefix: &str) -> Option<usize> {
    if values.is_empty() {
        return None;
    }

    let mut a = 0;
    let mut d = values.len() - 1;

    loop {
        let (oa, od) = compare_prefixes(prefix, values, a, d);
        match (oa, od) {
            (Less, Less) => return None,
            (Less, Equal) => {
                if a + 1 == d {
                    return Some(d);
                }

                let c = midpoint(a, d);
                let b = c.saturating_sub(1).max(a);
                let (ob, oc) = compare_prefixes(prefix, values, b, c);
                match (ob, oc) {
                    (Less, Less) => {
                        a = c;
                    }
                    (Less, Equal) => return Some(c),
                    (Equal, Equal) => {
                        d = c;
                    }
                    _ => unreachable!(),
                }
            }
            (Equal, Equal) => return Some(a),
            (Equal, Greater) => return Some(a),
            (Greater, Greater) => return None,
            (Less, Greater) => {
                let c = midpoint(a, d);
                let b = c.saturating_sub(1).max(a);
                let (ob, oc) = compare_prefixes(prefix, values, b, c);
                match (ob, oc) {
                    (Less, Less) => {
                        a = c;
                    }
                    (Less, Equal) => return Some(b),
                    (Equal, Equal) => {
                        d = b;
                    }
                    (Equal, Greater) => {
                        d = b;
                    }
                    (Greater, Greater) => {
                        d = b;
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

#[allow(clippy::match_same_arms)]
fn find_prefix_range_end<S: AsRef<str>>(values: &[S], prefix: &str) -> Option<usize> {
    if values.is_empty() {
        return None;
    }

    let mut a = 0;
    let mut d = values.len() - 1;

    loop {
        let (oa, od) = compare_prefixes(prefix, values, a, d);
        match (oa, od) {
            (Less, Less) => return None,
            (Less, Equal) => return Some(d),
            (Equal, Equal) => return Some(d),
            (Equal, Greater) => {
                if a + 1 == d {
                    return Some(d);
                }

                let b = midpoint(a, d);
                let c = b.saturating_add(1).min(d);
                let (ob, oc) = compare_prefixes(prefix, values, b, c);
                match (ob, oc) {
                    (Equal, Equal) => {
                        a = b;
                    }
                    (Equal, Greater) => return Some(b),
                    (Greater, Greater) => {
                        d = b;
                    }
                    _ => unreachable!(),
                }
            }
            (Greater, Greater) => return None,
            (Less, Greater) => {
                let b = midpoint(a, d);
                let c = b.saturating_add(1).min(d);
                let (ob, oc) = compare_prefixes(prefix, values, b, c);
                match (ob, oc) {
                    (Less, Less) => {
                        a = c;
                    }
                    (Less, Equal) => {
                        a = c;
                    }
                    (Equal, Equal) => {
                        a = c;
                    }
                    (Equal, Greater) => return Some(b),
                    (Greater, Greater) => {
                        d = b;
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

fn midpoint(start: usize, end: usize) -> usize {
    let distance = end - start;
    start + distance / 2
}

fn compare_prefixes<S: AsRef<str>>(
    needle: &str,
    haystacks: &[S],
    a: usize,
    b: usize,
) -> (Ordering, Ordering) {
    let haystack_a = haystacks[a].as_ref();
    let haystack_b = haystacks[b].as_ref();
    let oa = compare_prefix(needle, haystack_a);
    let ob = compare_prefix(needle, haystack_b);
    (oa, ob)
}

fn compare_prefix(needle: &str, haystack: &str) -> Ordering {
    if haystack < needle {
        Less
    } else if haystack.starts_with(needle) {
        Equal
    } else {
        Greater
    }
}
