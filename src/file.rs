use std::{
    collections::{btree_map, BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

use crate::{archive::unix_timestamp, error::Error, hash::Hash};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct File;

pub type FileHash = Hash<File>;

pub struct Metadata {
    pub inode: u64,
    pub mode: u32,
    pub group: u32,
    pub owner: u32,
    pub accessed: i64,
    pub created: i64,
    pub modified: i64,
}

impl Metadata {
    pub fn from_native(native: std::fs::Metadata) -> Result<Self, Error> {
        Ok(Metadata {
            inode: native.ino(),
            mode: native.mode(),
            group: native.gid(),
            owner: native.uid(),
            accessed: native.accessed().map(unix_timestamp)?,
            created: native.created().map(unix_timestamp)?,
            modified: native.modified().map(unix_timestamp)?,
        })
    }
}

pub enum Node {
    File {
        metadata: Metadata,
        hash: FileHash,
    },
    Symlink {
        metadata: Metadata,
        path: PathBuf,
    },
    Directory {
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
}

pub struct FileTree {
    root: BTreeMap<OsString, Node>,
    paths: HashMap<u64, PathBuf>,
}

impl FileTree {
    pub fn new() -> Self {
        FileTree {
            root: BTreeMap::new(),
            paths: HashMap::new(),
        }
    }

    pub fn insert(&mut self, path: &Path, node: Node) -> Result<(), Error> {
        let (keys, name) = path_keys(path)?;
        let mut children = &mut self.root;

        for key in keys.iter().cloned() {
            if let Some(Node::Directory {
                children: subdir_children,
                ..
            }) = children.get_mut(key)
            {
                children = subdir_children;
            } else {
                return Err(Error::invalid_path(path));
            }
        }

        if children.contains_key(name) {
            return Err(Error::path_already_exists(path));
        }

        self.paths.insert(node.metadata().inode, path.to_owned());
        children.insert(name.to_owned(), node);
        Ok(())
    }

    pub fn path(&self, inode: u64) -> Option<&Path> {
        self.paths.get(&inode).map(|path| path.as_path())
    }

    pub fn walk(&self) -> FileWalker<'_> {
        FileWalker {
            path: PathBuf::new(),
            dirs: vec![self.root.iter()],
        }
    }
}

pub struct FileWalker<'a> {
    path: PathBuf,
    dirs: Vec<btree_map::Iter<'a, OsString, Node>>,
}

impl<'a> Iterator for FileWalker<'a> {
    type Item = (PathBuf, &'a Node);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(layer) = self.dirs.last_mut() {
            if let Some((name, node)) = layer.next() {
                let node_path = self.path.join(name);

                if let Node::Directory { children, .. } = node {
                    self.path.push(name);
                    self.dirs.push(children.iter());
                }

                return Option::Some((node_path, node));
            }

            self.path.pop();
            self.dirs.pop();
        }

        Option::None
    }
}

fn path_keys(path: &Path) -> Result<(Vec<&OsStr>, &OsStr), Error> {
    let mut keys = path
        .components()
        .filter_map(get_normal_component)
        .collect::<Vec<_>>();
    let name = keys.pop().ok_or_else(|| Error::invalid_path(path))?;
    Ok((keys, name))
}

fn get_normal_component(component: Component) -> Option<&OsStr> {
    if let Component::Normal(s) = component {
        Option::Some(s)
    } else {
        Option::None
    }
}
