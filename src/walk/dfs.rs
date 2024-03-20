use std::{collections::btree_map, ffi::OsString, path::PathBuf};

use crate::file::{Node, NodeChildren};

#[derive(Debug)]
pub struct WalkNode<'a> {
    path: PathBuf,
    layers: Vec<btree_map::Iter<'a, OsString, Node>>,
}

impl<'a> WalkNode<'a> {
    pub fn new(children: &'a NodeChildren) -> WalkNode<'a> {
        WalkNode {
            path: PathBuf::new(),
            layers: vec![children.iter()],
        }
    }
}

impl<'a> Iterator for WalkNode<'a> {
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
