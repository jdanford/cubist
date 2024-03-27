use crate::prefix::{cmp_prefix as cmp, find_prefix_range as find};

fn words(input: &str) -> Vec<&str> {
    input.split_ascii_whitespace().collect()
}

#[test]
fn find_empty() {
    assert_eq!(find(&words(""), "", &cmp), None);
    assert_eq!(find(&words("a"), "", &cmp), Some(0..=0));
    assert_eq!(find(&words("a b"), "", &cmp), Some(0..=1));
    assert_eq!(find(&words("a b c"), "", &cmp), Some(0..=2));
    assert_eq!(find(&words("a b c d"), "", &cmp), Some(0..=3));
}

#[test]
fn find_0() {
    assert_eq!(find(&words(""), "a", &cmp), None);
    assert_eq!(find(&words(""), "b", &cmp), None);
    assert_eq!(find(&words(""), "c", &cmp), None);
}

#[test]
fn find_1_a() {
    assert_eq!(find(&words("a"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("b"), "a", &cmp), None);
    assert_eq!(find(&words("c"), "a", &cmp), None);
}

#[test]
fn find_1_b() {
    assert_eq!(find(&words("a"), "b", &cmp), None);
    assert_eq!(find(&words("b"), "b", &cmp), Some(0..=0));
    assert_eq!(find(&words("c"), "b", &cmp), None);
}

#[test]
fn find_1_c() {
    assert_eq!(find(&words("a"), "c", &cmp), None);
    assert_eq!(find(&words("b"), "c", &cmp), None);
    assert_eq!(find(&words("c"), "c", &cmp), Some(0..=0));
}

#[test]
fn find_2_a() {
    assert_eq!(find(&words("a a"), "a", &cmp), Some(0..=1));
    assert_eq!(find(&words("a b"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("a c"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("b b"), "a", &cmp), None);
    assert_eq!(find(&words("b c"), "a", &cmp), None);
    assert_eq!(find(&words("c c"), "a", &cmp), None);
}

#[test]
fn find_2_b() {
    assert_eq!(find(&words("a a"), "b", &cmp), None);
    assert_eq!(find(&words("a b"), "b", &cmp), Some(1..=1));
    assert_eq!(find(&words("a c"), "b", &cmp), None);
    assert_eq!(find(&words("b b"), "b", &cmp), Some(0..=1));
    assert_eq!(find(&words("b c"), "b", &cmp), Some(0..=0));
    assert_eq!(find(&words("c c"), "b", &cmp), None);
}

#[test]
fn find_2_c() {
    assert_eq!(find(&words("a a"), "c", &cmp), None);
    assert_eq!(find(&words("a b"), "c", &cmp), None);
    assert_eq!(find(&words("a c"), "c", &cmp), Some(1..=1));
    assert_eq!(find(&words("b b"), "c", &cmp), None);
    assert_eq!(find(&words("b c"), "c", &cmp), Some(1..=1));
    assert_eq!(find(&words("c c"), "c", &cmp), Some(0..=1));
}

#[test]
fn find_3_a() {
    assert_eq!(find(&words("a a a"), "a", &cmp), Some(0..=2));
    assert_eq!(find(&words("a a b"), "a", &cmp), Some(0..=1));
    assert_eq!(find(&words("a a c"), "a", &cmp), Some(0..=1));
    assert_eq!(find(&words("a b b"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("a b c"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("a c c"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("b b b"), "a", &cmp), None);
    assert_eq!(find(&words("b b c"), "a", &cmp), None);
    assert_eq!(find(&words("b c c"), "a", &cmp), None);
    assert_eq!(find(&words("c c c"), "a", &cmp), None);
}

#[test]
fn find_3_b() {
    assert_eq!(find(&words("a a a"), "b", &cmp), None);
    assert_eq!(find(&words("a a b"), "b", &cmp), Some(2..=2));
    assert_eq!(find(&words("a a c"), "b", &cmp), None);
    assert_eq!(find(&words("a b b"), "b", &cmp), Some(1..=2));
    assert_eq!(find(&words("a b c"), "b", &cmp), Some(1..=1));
    assert_eq!(find(&words("a c c"), "b", &cmp), None);
    assert_eq!(find(&words("b b b"), "b", &cmp), Some(0..=2));
    assert_eq!(find(&words("b b c"), "b", &cmp), Some(0..=1));
    assert_eq!(find(&words("b c c"), "b", &cmp), Some(0..=0));
    assert_eq!(find(&words("c c c"), "b", &cmp), None);
}

#[test]
fn find_3_c() {
    assert_eq!(find(&words("a a a"), "c", &cmp), None);
    assert_eq!(find(&words("a a b"), "c", &cmp), None);
    assert_eq!(find(&words("a a c"), "c", &cmp), Some(2..=2));
    assert_eq!(find(&words("a b b"), "c", &cmp), None);
    assert_eq!(find(&words("a b c"), "c", &cmp), Some(2..=2));
    assert_eq!(find(&words("a c c"), "c", &cmp), Some(1..=2));
    assert_eq!(find(&words("b b b"), "c", &cmp), None);
    assert_eq!(find(&words("b b c"), "c", &cmp), Some(2..=2));
    assert_eq!(find(&words("b c c"), "c", &cmp), Some(1..=2));
    assert_eq!(find(&words("c c c"), "c", &cmp), Some(0..=2));
}

#[test]
fn find_4_a() {
    assert_eq!(find(&words("a a a a"), "a", &cmp), Some(0..=3));
    assert_eq!(find(&words("a a a b"), "a", &cmp), Some(0..=2));
    assert_eq!(find(&words("a a a c"), "a", &cmp), Some(0..=2));
    assert_eq!(find(&words("a a b b"), "a", &cmp), Some(0..=1));
    assert_eq!(find(&words("a a b c"), "a", &cmp), Some(0..=1));
    assert_eq!(find(&words("a a c c"), "a", &cmp), Some(0..=1));
    assert_eq!(find(&words("a b b b"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("a b b c"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("a b c c"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("a c c c"), "a", &cmp), Some(0..=0));
    assert_eq!(find(&words("b b b b"), "a", &cmp), None);
    assert_eq!(find(&words("b b b c"), "a", &cmp), None);
    assert_eq!(find(&words("b b c c"), "a", &cmp), None);
    assert_eq!(find(&words("b c c c"), "a", &cmp), None);
    assert_eq!(find(&words("c c c c"), "a", &cmp), None);
}

#[test]
fn find_4_b() {
    assert_eq!(find(&words("a a a a"), "b", &cmp), None);
    assert_eq!(find(&words("a a a b"), "b", &cmp), Some(3..=3));
    assert_eq!(find(&words("a a a c"), "b", &cmp), None);
    assert_eq!(find(&words("a a b b"), "b", &cmp), Some(2..=3));
    assert_eq!(find(&words("a a b c"), "b", &cmp), Some(2..=2));
    assert_eq!(find(&words("a a c c"), "b", &cmp), None);
    assert_eq!(find(&words("a b b b"), "b", &cmp), Some(1..=3));
    assert_eq!(find(&words("a b b c"), "b", &cmp), Some(1..=2));
    assert_eq!(find(&words("a b c c"), "b", &cmp), Some(1..=1));
    assert_eq!(find(&words("a c c c"), "b", &cmp), None);
    assert_eq!(find(&words("b b b b"), "b", &cmp), Some(0..=3));
    assert_eq!(find(&words("b b b c"), "b", &cmp), Some(0..=2));
    assert_eq!(find(&words("b b c c"), "b", &cmp), Some(0..=1));
    assert_eq!(find(&words("b c c c"), "b", &cmp), Some(0..=0));
    assert_eq!(find(&words("c c c c"), "b", &cmp), None);
}

#[test]
fn find_4_c() {
    assert_eq!(find(&words("a a a a"), "c", &cmp), None);
    assert_eq!(find(&words("a a a b"), "c", &cmp), None);
    assert_eq!(find(&words("a a a c"), "c", &cmp), Some(3..=3));
    assert_eq!(find(&words("a a b b"), "c", &cmp), None);
    assert_eq!(find(&words("a a b c"), "c", &cmp), Some(3..=3));
    assert_eq!(find(&words("a a c c"), "c", &cmp), Some(2..=3));
    assert_eq!(find(&words("a b b b"), "c", &cmp), None);
    assert_eq!(find(&words("a b b c"), "c", &cmp), Some(3..=3));
    assert_eq!(find(&words("a b c c"), "c", &cmp), Some(2..=3));
    assert_eq!(find(&words("a c c c"), "c", &cmp), Some(1..=3));
    assert_eq!(find(&words("b b b b"), "c", &cmp), None);
    assert_eq!(find(&words("b b b c"), "c", &cmp), Some(3..=3));
    assert_eq!(find(&words("b b c c"), "c", &cmp), Some(2..=3));
    assert_eq!(find(&words("b c c c"), "c", &cmp), Some(1..=3));
    assert_eq!(find(&words("c c c c"), "c", &cmp), Some(0..=3));
}
