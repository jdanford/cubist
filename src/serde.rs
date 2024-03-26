use bincode::{DefaultOptions, Options};
use serde::{de::DeserializeOwned, Serialize};

use crate::error::Result;

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = bincode_options().serialize(value)?;
    Ok(bytes)
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let value = bincode_options().deserialize(bytes)?;
    Ok(value)
}

fn bincode_options() -> impl Options {
    DefaultOptions::new().with_varint_encoding()
}
