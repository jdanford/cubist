use std::{fmt::Display, ops::RangeInclusive, str::FromStr};

use crate::hash::{self, ShortHash};

pub fn parse_range_inclusive<N: PartialEq + PartialOrd + FromStr + Display>(
    s: &str,
    range: RangeInclusive<N>,
) -> Result<N, String> {
    let value: N = s.parse().map_err(|_| "invalid numeric value")?;
    if range.contains(&value) {
        Ok(value)
    } else {
        Err(format!(
            "{} is not in range {}-{}",
            value,
            range.start(),
            range.end(),
        ))
    }
}

pub fn parse_short_hash(s: &str) -> Result<ShortHash, String> {
    let len_range = hash::PREFIX_LENGTH_RANGE;
    let len = s.len();
    if len_range.contains(&len) {
        ShortHash::new(s.to_owned()).map_err(|_| "invalid characters in hash".to_string())
    } else {
        Err(format!(
            "hash has {} characters, expected {}-{}",
            len,
            len_range.start(),
            len_range.end(),
        ))
    }
}
