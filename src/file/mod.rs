mod archive;
mod metadata;
mod node;

use crate::hash::Hash;

pub use self::{archive::Archive, metadata::Metadata, node::Node};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct File;

pub type FileHash = Hash<File>;

impl FileHash {
    pub fn key(&self) -> String {
        format!("file/{}", self)
    }
}
