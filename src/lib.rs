#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::similar_names
)]

pub mod cli;

mod archive;
mod block;
mod error;
mod file;
mod hash;
mod serde;
mod stats;
mod storage;
