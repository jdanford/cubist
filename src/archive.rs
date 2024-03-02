use std::{
    collections::{btree_map, BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    error::{Error, Result},
    file::Node,
};

#[derive(Debug)]
pub struct Archive {
    root: BTreeMap<OsString, Node>,
    paths: HashMap<u64, PathBuf>,
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
        let root = Deserialize::deserialize(deserializer)?;
        Ok(Archive::from_root(root))
    }
}

impl Archive {
    pub fn new() -> Self {
        Archive {
            root: BTreeMap::new(),
            paths: HashMap::new(),
        }
    }

    pub fn from_root(root: BTreeMap<OsString, Node>) -> Self {
        let mut paths = HashMap::new();
        for (path, node) in FileWalker::from_root(&root) {
            paths.insert(node.metadata().inode, path);
        }

        Archive { root, paths }
    }

    pub fn insert(&mut self, path: &Path, node: Node) -> Result<()> {
        let (keys, name) = path_keys(path)?;

        let mut current_path = PathBuf::new();
        let mut children = &mut self.root;

        for key in keys.iter().copied() {
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
            return Err(Error::PathAlreadyArchived(path.to_owned()));
        }

        self.paths.insert(node.metadata().inode, path.to_owned());
        children.insert(name.to_owned(), node);
        Ok(())
    }

    pub fn path(&self, inode: u64) -> Option<&Path> {
        self.paths.get(&inode).map(PathBuf::as_path)
    }

    pub fn walk(&self) -> FileWalker<'_> {
        FileWalker::from_root(&self.root)
    }
}

pub struct FileWalker<'a> {
    path: PathBuf,
    layers: Vec<btree_map::Iter<'a, OsString, Node>>,
}

impl<'a> FileWalker<'a> {
    fn from_root(root: &'a BTreeMap<OsString, Node>) -> FileWalker<'a> {
        FileWalker {
            path: PathBuf::new(),
            layers: vec![root.iter()],
        }
    }
}

impl<'a> Iterator for FileWalker<'a> {
    type Item = (PathBuf, &'a Node);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(layer) = self.layers.last_mut() {
            if let Some((name, node)) = layer.next() {
                let node_path = self.path.join(name);

                if let Node::Directory { children, .. } = node {
                    self.path.push(name);
                    self.layers.push(children.iter());
                }

                return Option::Some((node_path, node));
            }

            self.path.pop();
            self.layers.pop();
        }

        Option::None
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
        Option::Some(s)
    } else {
        Option::None
    }
}
