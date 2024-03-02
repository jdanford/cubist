#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::new_without_default)]

pub mod error;
pub mod logger;
pub mod storage;
pub mod backup;
pub mod restore;
pub mod inspect;
pub mod cli;

mod file;
mod block;
mod hash;
