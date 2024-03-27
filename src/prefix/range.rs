use std::{
    cmp::Ordering::{self, Equal, Greater, Less},
    ops::RangeInclusive,
};

pub fn find_prefix_range<T, R, F>(
    values: &[R],
    prefix: &T,
    cmp: &F,
) -> Option<RangeInclusive<usize>>
where
    T: ?Sized,
    R: AsRef<T>,
    F: Fn(&T, &T) -> Ordering,
{
    let start = find_prefix_range_start(values, prefix, cmp)?;
    let end = find_prefix_range_end(values, prefix, cmp)?;
    Some(start..=end)
}

#[allow(clippy::match_same_arms)]
fn find_prefix_range_start<T, R, F>(values: &[R], prefix: &T, cmp: &F) -> Option<usize>
where
    T: ?Sized,
    R: AsRef<T>,
    F: Fn(&T, &T) -> Ordering,
{
    if values.is_empty() {
        return None;
    }

    let mut a = 0;
    let mut d = values.len() - 1;

    loop {
        let oa = cmp(values[a].as_ref(), prefix);
        let od = cmp(values[d].as_ref(), prefix);
        match (oa, od) {
            (Less, Less) => return None,
            (Less, Equal) => {
                if a + 1 == d {
                    return Some(d);
                }

                let (b, c) = midpoints(a, d);
                let ob = cmp(values[b].as_ref(), prefix);
                let oc = cmp(values[c].as_ref(), prefix);
                match (ob, oc) {
                    (Less, Less) => a = c,
                    (Less, Equal) => return Some(c),
                    (Equal, Equal) => d = b,
                    _ => panic!("array is not sorted"),
                }
            }
            (Equal, Equal) => return Some(a),
            (Equal, Greater) => return Some(a),
            (Greater, Greater) => return None,
            (Less, Greater) => {
                if a + 1 == d {
                    return None;
                }

                let (b, c) = midpoints(a, d);
                let ob = cmp(values[b].as_ref(), prefix);
                let oc = cmp(values[c].as_ref(), prefix);
                match (ob, oc) {
                    (Less, Less) => a = c,
                    (Less, Equal) => return Some(c),
                    (Equal, Equal) => d = b,
                    (Equal, Greater) => d = b,
                    (Greater, Greater) => d = b,
                    (Less, Greater) => return None,
                    _ => panic!("array is not sorted"),
                }
            }
            _ => panic!("array is not sorted"),
        }
    }
}

#[allow(clippy::match_same_arms)]
fn find_prefix_range_end<T, R, F>(values: &[R], prefix: &T, cmp: &F) -> Option<usize>
where
    T: ?Sized,
    R: AsRef<T>,
    F: Fn(&T, &T) -> Ordering,
{
    if values.is_empty() {
        return None;
    }

    let mut a = 0;
    let mut d = values.len() - 1;

    loop {
        let oa = cmp(values[a].as_ref(), prefix);
        let od = cmp(values[d].as_ref(), prefix);
        match (oa, od) {
            (Less, Less) => return None,
            (Less, Equal) => return Some(d),
            (Equal, Equal) => return Some(d),
            (Equal, Greater) => {
                if a + 1 == d {
                    return Some(a);
                }

                let (b, c) = midpoints(a, d);
                let ob = cmp(values[b].as_ref(), prefix);
                let oc = cmp(values[c].as_ref(), prefix);
                match (ob, oc) {
                    (Equal, Equal) => a = c,
                    (Equal, Greater) => return Some(b),
                    (Greater, Greater) => d = b,
                    _ => panic!("array is not sorted"),
                }
            }
            (Greater, Greater) => return None,
            (Less, Greater) => {
                if a + 1 == d {
                    return None;
                }

                let (b, c) = midpoints(a, d);
                let ob = cmp(values[b].as_ref(), prefix);
                let oc = cmp(values[c].as_ref(), prefix);
                match (ob, oc) {
                    (Less, Less) => a = c,
                    (Less, Equal) => a = c,
                    (Equal, Equal) => a = c,
                    (Equal, Greater) => return Some(b),
                    (Greater, Greater) => d = b,
                    (Less, Greater) => return None,
                    _ => panic!("array is not sorted"),
                }
            }
            _ => panic!("array is not sorted"),
        }
    }
}

fn midpoints(a: usize, d: usize) -> (usize, usize) {
    let b = midpoint(a, d);
    let c = b.saturating_add(1).min(d);
    (b, c)
}

fn midpoint(start: usize, end: usize) -> usize {
    let distance = end - start;
    start + distance / 2
}
