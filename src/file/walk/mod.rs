mod bfs;
mod dfs;

use std::{fmt, path::PathBuf};

use clap::ValueEnum;

use super::{Node, NodeChildren};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum WalkOrder {
    DepthFirst,
    BreadthFirst,
}

impl fmt::Display for WalkOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalkOrder::DepthFirst => write!(f, "depth-first"),
            WalkOrder::BreadthFirst => write!(f, "breadth-first"),
        }
    }
}

#[derive(Debug)]
pub enum WalkNode<'a> {
    Single(Option<&'a Node>),
    DepthFirst(dfs::WalkNode<'a>),
    BreadthFirst(bfs::WalkNode<'a>),
}

impl<'a> WalkNode<'a> {
    pub fn new(node: &'a Node, order: WalkOrder) -> Self {
        if let Node::Directory { children, .. } = node {
            WalkNode::from_children(children, order)
        } else {
            WalkNode::Single(Some(node))
        }
    }

    pub fn from_children(children: &'a NodeChildren, order: WalkOrder) -> Self {
        match order {
            WalkOrder::DepthFirst => WalkNode::DepthFirst(dfs::WalkNode::new(children)),
            WalkOrder::BreadthFirst => WalkNode::BreadthFirst(bfs::WalkNode::new(children)),
        }
    }
}

impl<'a> Iterator for WalkNode<'a> {
    type Item = (PathBuf, &'a Node);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            WalkNode::Single(maybe_node) => maybe_node.map(|node| (PathBuf::new(), node)),
            WalkNode::DepthFirst(iter) => iter.next(),
            WalkNode::BreadthFirst(iter) => iter.next(),
        }
    }
}
