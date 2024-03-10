use std::{
    collections::{BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    error::{Error, Result},
    file::Node,
    walker::FileWalker,
};

#[derive(Debug)]
pub struct Archive {
    root: BTreeMap<OsString, Node>,
    paths: HashMap<u64, PathBuf>,
}

impl Archive {
    pub fn new() -> Self {
        Archive {
            root: BTreeMap::new(),
            paths: HashMap::new(),
        }
    }

    fn from_root(root: BTreeMap<OsString, Node>) -> Self {
        let mut paths = HashMap::new();
        for (path, node) in FileWalker::from_root(&root) {
            paths.insert(node.metadata().inode, path);
        }

        Archive { root, paths }
    }

    pub fn insert(&mut self, path: PathBuf, node: Node) -> Result<()> {
        let (keys, name) = path_keys(&path)?;
        let mut current_path = PathBuf::new();
        let mut children = &mut self.root;

        for key in keys {
            current_path.push(key);
            match children.get_mut(key) {
                Some(Node::Directory {
                    children: grandchildren,
                    ..
                }) => {
                    children = grandchildren;
                }
                Some(_) => {
                    return Err(Error::FileIsNotDirectory(current_path));
                }
                None => {
                    return Err(Error::DirectoryDoesNotExist(current_path));
                }
            }
        }

        if children.contains_key(name) {
            return Err(Error::PathAlreadyArchived(path));
        }

        let name = name.to_owned();
        self.paths.insert(node.metadata().inode, path);
        children.insert(name, node);
        Ok(())
    }

    pub fn path(&self, inode: u64) -> Option<&Path> {
        self.paths.get(&inode).map(PathBuf::as_path)
    }

    pub fn walk(&self) -> FileWalker<'_> {
        FileWalker::from_root(&self.root)
    }
}

impl Serialize for Archive {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        Serialize::serialize(&self.root, serializer)
    }
}

impl<'de> Deserialize<'de> for Archive {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Archive, D::Error> {
        Deserialize::deserialize(deserializer).map(Archive::from_root)
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
