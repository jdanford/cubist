use std::{
    collections::{btree_map, BTreeMap},
    ffi::OsString,
    path::PathBuf,
};

use crate::file::Node;

#[derive(Debug)]
pub struct FileWalker<'a> {
    path: PathBuf,
    layers: Vec<btree_map::Iter<'a, OsString, Node>>,
}

impl<'a> FileWalker<'a> {
    pub fn new(root: &'a BTreeMap<OsString, Node>) -> FileWalker<'a> {
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

                return Some((node_path, node));
            }

            self.path.pop();
            self.layers.pop();
        }

        None
    }
}
