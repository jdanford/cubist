#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::new_without_default,
    clippy::no_effect_underscore_binding,
    clippy::similar_names
)]

pub mod backup;
pub mod cli;
pub mod error;
pub mod logger;
pub mod restore;
pub mod storage;

mod archive;
mod block;
mod file;
mod hash;
mod refs;
mod serde;
mod stats;
