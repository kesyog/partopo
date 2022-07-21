# partopo

[![crates.io](https://img.shields.io/crates/v/partopo?style=flat-square)](https://crates.io/crates/partopo)
[![Apache 2.0 license](https://img.shields.io/github/license/kesyog/partopo?style=flat-square)](./LICENSE)
[![docs.rs](https://img.shields.io/docsrs/partopo?style=flat-square)](https://docs.rs/partopo)

Execute work described by a dependency graph using either a single-threaded worker or in parallel
using a threadpool.

This is an implementation of [Kahn's algorithm](https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm)
for topological sorting minimally adapted to be run in parallel.

## Usage

```rust
use partopo::{Dag, Node};
use std::time::Instant;
use std::{thread, time::Duration};

// Construct a DAG:
// 1 -> 2
// 1 -> 3
// 4 -> 5
// 2 -> 5

let mut dag: Dag<usize> = Dag::new();
let idx1 = dag.add_node(Node::new(1));
let (_, idx2) = dag.add_child(idx1, (), Node::new(2));
let (_, _idx3) = dag.add_child(idx1, (), Node::new(3));
let idx4 = dag.add_node(Node::new(4));
let (_, idx5) = dag.add_child(idx4, (), Node::new(5));
dag.add_edge(idx2, idx5, ()).unwrap();

// Example work function
fn do_work(data: usize) {
    thread::sleep(Duration::from_millis(1000));
    println!("{}", data);
}

// Execute work on a threadpool
partopo::par_execute(dag, do_work);
```

## Disclaimer

This is not an officially supported Google product
