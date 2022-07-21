// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.!
//! # partopo
//!
//! Execute work described by a dependency graph using either a single-threaded worker or in
//! parallel using a threadpool.
//!
//! ## Usage
//!
//! First, create a [`DependencyDag<T>`] dependency graph using API's from [`daggy::stable_dag::StableDag`]:
//!
//! ```rust
//! use partopo::{DependencyDag, Node};
//!
//! // Construct a DAG:
//! // 1 -> 2
//! // 1 -> 3
//! // 4 -> 5
//! // 2 -> 5
//!
//! let mut dag: DependencyDag<usize> = DependencyDag::new();
//! let idx1 = dag.add_node(Node::new(1));
//! let (_, idx2) = dag.add_child(idx1, (), Node::new(2));
//! let (_, _idx3) = dag.add_child(idx1, (), Node::new(3));
//! let idx4 = dag.add_node(Node::new(4));
//! let (_, idx5) = dag.add_child(idx4, (), Node::new(5));
//! dag.add_edge(idx2, idx5, ()).unwrap();
//! ```
//!
//! Run through the graph using a single-threaded worker:
//!
//! ```rust
//! # use partopo::{DependencyDag, Node};
//! # let mut dag: DependencyDag<usize> = DependencyDag::new();
//! # let idx1 = dag.add_node(Node::new(1));
//! # let (_, idx2) = dag.add_child(idx1, (), Node::new(2));
//! # let (_, _idx3) = dag.add_child(idx1, (), Node::new(3));
//! # let idx4 = dag.add_node(Node::new(4));
//! # let (_, idx5) = dag.add_child(idx4, (), Node::new(5));
//! # dag.add_edge(idx2, idx5, ()).unwrap();
//! fn do_work(data: usize) {
//!     println!("{}", data);
//! }
//!
//! partopo::execute(dag, do_work);
//! ```
//!
//! The same example can be adapted to run on multiple threads:
//!
//! ```rust
//! # use partopo::{DependencyDag, Node};
//! # let mut dag: DependencyDag<usize> = DependencyDag::new();
//! # let idx1 = dag.add_node(Node::new(1));
//! # let (_, idx2) = dag.add_child(idx1, (), Node::new(2));
//! # let (_, _idx3) = dag.add_child(idx1, (), Node::new(3));
//! # let idx4 = dag.add_node(Node::new(4));
//! # let (_, idx5) = dag.add_child(idx4, (), Node::new(5));
//! # dag.add_edge(idx2, idx5, ()).unwrap();
//! # fn do_work(data: usize) {
//! #    println!("{}", data);
//! # }
//! partopo::par_execute(dag, do_work);
//! ```
pub use daggy;
pub use daggy::petgraph;

use daggy::{stable_dag::StableDag, NodeIndex};
use petgraph::Direction;
use std::collections::VecDeque;

/// Container for the directed acyclic graph that represent data and their dependencies.
///
/// This is a wrapper type around [`StableDag`](daggy::stable_dag::StableDag) with no edge weights
/// Using a `StableDag` ensures that we can safely remove nodes without invalidating `NodeIndex`'s
pub type DependencyDag<T> = StableDag<Node<T>, ()>;

/// A node in the [`DependencyDag<T>`] dependency graph.
///
/// The node contains data to be used by the work function passed to [`execute`] or [`par_execute`].
#[derive(Debug, Clone)]
pub struct Node<T> {
    // Data is stored with an Option so that we can move it out of the node while leaving the node
    // intact while the work is being done
    data: Option<T>,
}

impl<T> Node<T> {
    /// Create a new node with data
    ///
    /// The data is passed to the work function passed to [`execute`] or [`par_execute`]
    pub fn new(data: T) -> Self {
        Self { data: Some(data) }
    }
}

/// Returns whether a node in a graph has no remaining dependencies
fn is_node_free<T>(graph: &DependencyDag<T>, node: NodeIndex) -> bool {
    graph
        .graph()
        .edges_directed(node, Direction::Incoming)
        .next()
        == None
}

/// Remove processed node from graph and return the nodes that are newly-available
fn remove_processed_node<T>(
    graph: &mut DependencyDag<T>,
    node: NodeIndex,
) -> impl Iterator<Item = NodeIndex> + '_ {
    // False positive: need to give up immutable borrow of graph before removing node from graph
    #[allow(clippy::needless_collect)]
    let dependents: Vec<NodeIndex> = graph
        .graph()
        .neighbors_directed(node, Direction::Outgoing)
        .collect();

    graph.remove_node(node).expect("Node not in graph");

    // Find nodes that no longer have any dependencies
    dependents
        .into_iter()
        .filter(|idx| is_node_free(graph, *idx))
}

/// Return nodes in graph that do not have any dependencies
fn free_nodes<T>(graph: &DependencyDag<T>) -> impl Iterator<Item = NodeIndex> + '_ {
    graph
        .graph()
        .node_indices()
        .filter(|idx| is_node_free(graph, *idx))
}

/// Run the given work function on each node in the dependency graph using a single thread.
///
/// The nodes are processed in an order that respects the dependencies.
/// The current implementation uses a topological sort via [Kahn's algorithm](https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm>).
pub fn execute<T, F>(mut graph: DependencyDag<T>, mut work_func: F)
where
    F: FnMut(T),
{
    let mut work_queue: VecDeque<NodeIndex> = free_nodes(&graph).collect();
    while let Some(node) = work_queue.pop_front() {
        let item = graph
            .node_weight_mut(node)
            .expect("Node is a member of graph")
            .data
            .take()
            .expect("Node is only executed once");
        work_func(item);
        // Schedule any newly-available work
        work_queue.extend(remove_processed_node(&mut graph, node));
    }
}

/// Run the given work function on each node in the dependency graph.
///
/// The nodes are processed in an order that respects the dependencies. The work is parallelized on
/// a threadpool sized to the number of CPU's on the system.
/// The current implementation uses a parallel version of [Kahn's algorithm](https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm).
pub fn par_execute<T, F>(mut graph: DependencyDag<T>, work_func: F)
where
    T: Send,
    F: Fn(T) + Send + Clone,
{
    let mut work_queue: VecDeque<NodeIndex> = free_nodes(&graph).collect();

    rayon::scope(move |s| {
        let (tx, rx) = crossbeam_channel::unbounded::<NodeIndex>();

        while graph.node_count() > 0 {
            while let Some(node) = work_queue.pop_front() {
                let item = graph
                    .node_weight_mut(node)
                    .expect("Node is a member of graph")
                    .data
                    .take()
                    .expect("Node is only executed once");
                let tx = tx.clone();
                let work_func = work_func.clone();
                s.spawn(move |_| {
                    work_func(item);
                    tx.send(node)
                        .expect("Send to unbounded channel should not fail");
                });
            }
            // Poll for completed work and schedule any newly-available work
            while let Ok(completed_node_idx) = rx.try_recv() {
                work_queue.extend(remove_processed_node(&mut graph, completed_node_idx));
            }
        }
    });
}
