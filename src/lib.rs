#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::new_without_default,
    clippy::similar_names
)]

pub mod cli;

mod archive;
mod block;
mod error;
mod file;
mod hash;
mod logger;
mod serde;
mod stats;
mod storage;
