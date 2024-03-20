use std::{collections::VecDeque, path::PathBuf};

use crate::file::{Node, NodeChildren};

#[derive(Debug)]
pub struct WalkNode<'a> {
    nodes: VecDeque<(PathBuf, &'a Node)>,
}

impl<'a> WalkNode<'a> {
    pub fn new(children: &'a NodeChildren) -> WalkNode<'a> {
        let nodes = children
            .iter()
            .map(|(name, child)| (PathBuf::from(name), child))
            .collect();
        WalkNode { nodes }
    }
}

impl<'a> Iterator for WalkNode<'a> {
    type Item = (PathBuf, &'a Node);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((path, node)) = self.nodes.pop_front() {
            if let Node::Directory { children, .. } = node {
                for (name, child) in children {
                    let child_path = path.join(name);
                    self.nodes.push_back((child_path, child));
                }
            }

            return Some((path, node));
        }

        None
    }
}
