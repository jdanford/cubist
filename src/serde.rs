use std::io::Cursor;

use serde::{de::DeserializeOwned, Serialize};

use crate::error::Result;

pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    let mut data = vec![];
    ciborium::into_writer(value, &mut data).unwrap();
    data
}

pub fn deserialize<T: DeserializeOwned>(bytes: Vec<u8>) -> Result<T> {
    let reader = Cursor::new(bytes);
    let value = ciborium::from_reader(reader)?;
    Ok(value)
}