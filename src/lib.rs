#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

pub mod backup;
mod block;
pub mod cli;
pub mod error;
mod file;
mod hash;
pub mod logger;
pub mod restore;
pub mod storage;
