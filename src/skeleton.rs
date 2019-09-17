use std::collections::BTreeMap;
use std::collections::BTreeSet;
use super::bag::BTreeBag;
use preserves::value::AValue;

pub enum Event {
    Removed,
    Message,
    Added,
}

type Path = Vec<usize>;
type Paths = Vec<Path>;
type Captures = Vec<AValue>;

trait HandleEvent {
    fn handle_event<'a>(self, captures: &Captures);
}

pub enum Skeleton {
    Blank,
    Guarded(Guard, Vec<Skeleton>)
}

pub struct AnalysisResults {
    skeleton: Skeleton,
    const_paths: Paths,
    const_vals: Vec<AValue>,
    capture_paths: Paths,
    assertion: AValue,
}

pub struct Index {
    all_assertions: BTreeBag<AValue>,
}

impl Index {
    pub fn new() -> Self {
        Index{ all_assertions: BTreeBag::new() }
    }

    // pub fn add_handler(analysis_results: AnalysisResults, 
}

struct Node {
    continuation: Continuation,
    edges: BTreeMap<Selector, BTreeMap<Guard, Node>>,
}

struct Continuation {
    cached_assertions: BTreeSet<AValue>,
    leaf_map: BTreeMap<Paths, BTreeMap<Vec<AValue>, Leaf>>,
}

struct Selector {
    pop_count: usize,
    index: usize,
}

enum Guard {
    Rec(AValue, usize),
    Seq(usize),
}

struct Leaf { // aka Topic
    cached_assertions: BTreeSet<AValue>,
    handler_map: BTreeMap<Paths, Handler>,
}

struct Handler {
    cached_captures: BTreeBag<Captures>,
    callbacks: BTreeSet<Box<dyn HandleEvent>>,
}
