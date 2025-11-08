use bincode::config::{Configuration, standard};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::Result;

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = bincode::serde::encode_to_vec(value, bincode_config())?;
    Ok(bytes)
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, _) = bincode::serde::decode_from_slice(bytes, bincode_config())?;
    Ok(value)
}

fn bincode_config() -> Configuration {
    standard()
}
