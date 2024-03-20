use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    error::{Error, Result},
    file::{Node, NodeChildren},
};

use super::walk::{WalkNode, WalkOrder};

#[derive(Debug)]
pub struct FileTree {
    children: NodeChildren,
    paths: HashMap<u64, PathBuf>,
}

impl FileTree {
    pub fn new() -> Self {
        FileTree {
            children: NodeChildren::new(),
            paths: HashMap::new(),
        }
    }

    fn from_children(children: NodeChildren) -> Self {
        let mut paths = HashMap::new();
        let walker = WalkNode::from_children(&children, WalkOrder::DepthFirst);

        for (path, node) in walker {
            paths.insert(node.metadata().inode, path);
        }

        FileTree { children, paths }
    }

    pub fn get(&self, path: &Path) -> Option<&Node> {
        let (keys, name) = path_keys(path).ok()?;
        let mut subtree = &self.children;

        for key in keys {
            match subtree.get(key) {
                Some(Node::Directory { children, .. }) => {
                    subtree = children;
                }
                _ => {
                    return None;
                }
            }
        }

        subtree.get(name)
    }

    pub fn insert(&mut self, path: PathBuf, node: Node) -> Result<()> {
        let (keys, name) = path_keys(&path)?;
        let mut current_path = PathBuf::new();
        let mut subtree = &mut self.children;

        for key in keys {
            current_path.push(key);
            match subtree.get_mut(key) {
                Some(Node::Directory { children, .. }) => {
                    subtree = children;
                }
                Some(_) => {
                    return Err(Error::FileIsNotDirectory(current_path));
                }
                None => {
                    return Err(Error::FileDoesNotExist(current_path));
                }
            }
        }

        if subtree.contains_key(name) {
            return Err(Error::PathAlreadyArchived(path));
        }

        let name = name.to_owned();
        self.paths.insert(node.metadata().inode, path);
        subtree.insert(name, node);
        Ok(())
    }

    pub fn path(&self, inode: u64) -> Option<&Path> {
        self.paths.get(&inode).map(PathBuf::as_path)
    }
}

impl Serialize for FileTree {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        Serialize::serialize(&self.children, serializer)
    }
}

impl<'de> Deserialize<'de> for FileTree {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<FileTree, D::Error> {
        Deserialize::deserialize(deserializer).map(FileTree::from_children)
    }
}

fn path_keys(path: &Path) -> Result<(Vec<&OsStr>, &OsStr)> {
    let mut keys = path
        .components()
        .filter_map(get_normal_component)
        .collect::<Vec<_>>();
    let name = keys.pop().ok_or(Error::EmptyPath)?;
    Ok((keys, name))
}

fn get_normal_component(component: Component) -> Option<&OsStr> {
    if let Component::Normal(s) = component {
        Some(s)
    } else {
        None
    }
}
