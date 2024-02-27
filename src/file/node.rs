use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use serde::{Deserialize, Serialize};

use super::{metadata::Metadata, FileHash};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Node {
    File {
        #[serde(flatten)]
        metadata: Metadata,
        hash: FileHash,
    },
    Symlink {
        #[serde(flatten)]
        metadata: Metadata,
        path: PathBuf,
    },
    Directory {
        #[serde(flatten)]
        metadata: Metadata,
        children: BTreeMap<OsString, Node>,
    },
}

impl Node {
    pub fn metadata(&self) -> &Metadata {
        match self {
            Node::File { metadata, .. } => metadata,
            Node::Symlink { metadata, .. } => metadata,
            Node::Directory { metadata, .. } => metadata,
        }
    }

    // pub fn is_file(&self) -> bool {
    //     matches!(self, Node::File { .. })
    // }

    pub fn is_symlink(&self) -> bool {
        matches!(self, Node::Symlink { .. })
    }

    // pub fn is_directory(&self) -> bool {
    //     matches!(self, Node::Directory { .. })
    // }
}
