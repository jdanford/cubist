#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::similar_names
)]

pub mod cli;

mod arc;
mod archive;
mod block;
mod compression;
mod error;
mod file;
mod hash;
mod keys;
mod locks;
mod ops;
mod prefix;
mod serde;
mod stats;
mod storage;
