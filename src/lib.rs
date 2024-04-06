#![deny(unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::new_without_default,
    clippy::module_name_repetitions,
    clippy::similar_names
)]

pub mod cli;

mod arc;
mod archive;
mod block;
mod compression;
mod entity;
mod error;
mod file;
mod format;
mod hash;
mod locks;
mod logger;
mod ops;
mod prefix;
mod serde;
mod stats;
mod storage;
