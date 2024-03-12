use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::hash::Hash;

use super::metadata::Metadata;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Node {
    File {
        #[serde(flatten)]
        metadata: Metadata,
        hash: Option<Hash>,
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
            Node::File { metadata, .. }
            | Node::Symlink { metadata, .. }
            | Node::Directory { metadata, .. } => metadata,
        }
    }

    pub fn file_type(&self) -> FileType {
        match self {
            Node::File { .. } => FileType::File,
            Node::Symlink { .. } => FileType::Symlink,
            Node::Directory { .. } => FileType::Directory,
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum FileType {
    File,
    Symlink,
    Directory,
}

impl FileType {
    #[allow(dead_code)]
    pub fn is_file(&self) -> bool {
        *self == FileType::File
    }

    pub fn is_symlink(&self) -> bool {
        *self == FileType::Symlink
    }

    #[allow(dead_code)]
    pub fn is_directory(&self) -> bool {
        *self == FileType::Directory
    }
}
