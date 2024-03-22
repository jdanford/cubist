use std::{fmt::Display, ops::RangeInclusive, str::FromStr};

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
