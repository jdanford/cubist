use std::env::VarError;

use crate::error::{Error, Result};

pub fn var(name: &str) -> Result<String> {
    std::env::var(name).map_err(|err| {
        if let VarError::NotPresent = err {
            Error::MissingEnvVar(name.to_string())
        } else {
            Error::other(err)
        }
    })
}
