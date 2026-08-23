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
enum WalkInner<'a> {
    Done,
    DepthFirst(dfs::WalkNode<'a>),
    BreadthFirst(bfs::WalkNode<'a>),
}

#[derive(Debug)]
pub struct WalkNode<'a> {
    first: Option<&'a Node>,
    rest: WalkInner<'a>,
}

impl<'a> WalkNode<'a> {
    pub fn new(node: &'a Node, order: WalkOrder) -> Self {
        let rest = if let Node::Directory { children, .. } = node {
            walk_children(children, order)
        } else {
            WalkInner::Done
        };

        WalkNode {
            first: Some(node),
            rest,
        }
    }

    pub fn from_children(children: &'a NodeChildren, order: WalkOrder) -> Self {
        WalkNode {
            first: None,
            rest: walk_children(children, order),
        }
    }
}

fn walk_children(children: &NodeChildren, order: WalkOrder) -> WalkInner<'_> {
    match order {
        WalkOrder::DepthFirst => WalkInner::DepthFirst(dfs::WalkNode::new(children)),
        WalkOrder::BreadthFirst => WalkInner::BreadthFirst(bfs::WalkNode::new(children)),
    }
}

impl<'a> Iterator for WalkNode<'a> {
    type Item = (PathBuf, &'a Node);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some((PathBuf::new(), first));
        }

        match &mut self.rest {
            WalkInner::Done => None,
            WalkInner::DepthFirst(iter) => iter.next(),
            WalkInner::BreadthFirst(iter) => iter.next(),
        }
    }
}
