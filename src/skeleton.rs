use super::ConnId;
use super::packets::Assertion;
use super::packets::Captures;
use super::packets::EndpointName;
use super::packets::Event;

use preserves::value::{Map, Set, Value, NestedValue};
use std::collections::btree_map::Entry;

type Bag<A> = super::bag::BTreeBag<A>;

type Path = Vec<usize>;
type Paths = Vec<Path>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Endpoint {
    connection: ConnId,
    name: EndpointName,
}

pub enum Skeleton {
    Blank,
    Guarded(Guard, Vec<Skeleton>)
}

pub struct AnalysisResults {
    skeleton: Skeleton,
    const_paths: Paths,
    const_vals: Vec<Assertion>,
    capture_paths: Paths,
    assertion: Assertion,
}

#[derive(Debug)]
pub struct Index {
    all_assertions: Bag<Assertion>,
    root: Node,
}

impl Index {
    pub fn new() -> Self {
        Index{ all_assertions: Bag::new(), root: Node::new(Continuation::new(Set::new())) }
    }

    pub fn add_endpoint(&mut self, analysis_results: AnalysisResults, endpoint: Endpoint)
                        -> Vec<Event>
    {
        let continuation = self.root.extend(&analysis_results.skeleton);
        let continuation_cached_assertions = &continuation.cached_assertions;
        let const_val_map =
            continuation.leaf_map.entry(analysis_results.const_paths.clone()).or_insert_with(|| {
                let mut cvm = Map::new();
                for a in continuation_cached_assertions {
                    let key = project_paths(a.unscope(), &analysis_results.const_paths);
                    cvm.entry(key).or_insert_with(Leaf::new).cached_assertions.insert(a.clone());
                }
                cvm
            });
        let capture_paths = analysis_results.capture_paths;
        let leaf = const_val_map.entry(analysis_results.const_vals).or_insert_with(Leaf::new);
        let leaf_cached_assertions = &leaf.cached_assertions;
        let endpoints = leaf.endpoints_map.entry(capture_paths.clone()).or_insert_with(|| {
            let mut b = Bag::new();
            for a in leaf_cached_assertions {
                let (restriction_paths, term) = a.unpack();
                if is_unrestricted(&capture_paths, restriction_paths) {
                    let captures = project_paths(term, &capture_paths);
                    *b.entry(captures).or_insert(0) += 1;
                }
            }
            Endpoints::new(b)
        });
        let endpoint_name = endpoint.name.clone();
        endpoints.endpoints.insert(endpoint);
        endpoints.cached_captures.into_iter()
            .map(|(cs,_)| Event::Add(endpoint_name.clone(), cs.clone()))
            .collect()
    }

    pub fn remove_endpoint(&mut self, analysis_results: AnalysisResults, endpoint: Endpoint) {
        let continuation = self.root.extend(&analysis_results.skeleton);
        if let Entry::Occupied(mut const_val_map_entry)
            = continuation.leaf_map.entry(analysis_results.const_paths)
        {
            let const_val_map = const_val_map_entry.get_mut();
            if let Entry::Occupied(mut leaf_entry)
                = const_val_map.entry(analysis_results.const_vals)
            {
                let leaf = leaf_entry.get_mut();
                if let Entry::Occupied(mut endpoints_entry)
                    = leaf.endpoints_map.entry(analysis_results.capture_paths)
                {
                    let endpoints = endpoints_entry.get_mut();
                    endpoints.endpoints.remove(&endpoint);
                    if endpoints.endpoints.is_empty() {
                        endpoints_entry.remove_entry();
                    }
                }
                if leaf.is_empty() {
                    leaf_entry.remove_entry();
                }
            }
            if const_val_map.is_empty() {
                const_val_map_entry.remove_entry();
            }
        }
    }
}

#[derive(Debug)]
struct Node {
    continuation: Continuation,
    edges: Map<Selector, Map<Guard, Node>>,
}

impl Node {
    fn new(continuation: Continuation) -> Self {
        Node { continuation, edges: Map::new() }
    }

    fn extend(&mut self, skeleton: &Skeleton) -> &mut Continuation {
        let (_pop_count, final_node) = self.extend_walk(&mut Vec::new(), 0, 0, skeleton);
        &mut final_node.continuation
    }

    fn extend_walk(&mut self, path: &mut Path, pop_count: usize, index: usize, skeleton: &Skeleton)
                   -> (usize, &mut Node) {
        match skeleton {
            Skeleton::Blank => (pop_count, self),
            Skeleton::Guarded(cls, kids) => {
                let selector = Selector { pop_count, index };
                let continuation = &self.continuation;
                let table = self.edges.entry(selector).or_insert_with(Map::new);
                let mut next_node = table.entry(cls.clone()).or_insert_with(|| {
                    Self::new(Continuation::new(
                        continuation.cached_assertions.iter()
                            .filter(|a| {
                                Some(cls) == class_of(project_path(a.unscope(), path)).as_ref() })
                            .cloned()
                            .collect()))
                });
                let mut pop_count = 0;
                for (index, kid) in kids.iter().enumerate() {
                    path.push(index);
                    let (pc, nn) = next_node.extend_walk(path, pop_count, index, kid);
                    pop_count = pc;
                    next_node = nn;
                    path.pop();
                }
                (pop_count + 1, next_node)
            }
        }
    }
}

fn class_of(v: &Assertion) -> Option<Guard> {
    match v.value() {
        Value::Sequence(ref vs) => Some(Guard::Seq(vs.len())),
        Value::Record((ref l, ref fs)) => Some(Guard::Rec(l.clone(), fs.len())),
        _ => None,
    }
}

fn project_path<'a>(v: &'a Assertion, p: &Path) -> &'a Assertion {
    let mut v = v;
    for i in p {
        v = step(v, *i);
    }
    v
}

fn project_paths<'a>(v: &'a Assertion, ps: &Paths) -> Vec<Assertion> {
    ps.iter().map(|p| project_path(v, p)).cloned().collect()
}

fn step(v: &Assertion, i: usize) -> &Assertion {
    match v.value() {
        Value::Sequence(ref vs) => &vs[i],
        Value::Record((_, ref fs)) => &fs[i],
        _ => panic!("step: non-sequence, non-record {:?}", v)
    }
}

#[derive(Debug)]
struct Continuation {
    cached_assertions: Set<CachedAssertion>,
    leaf_map: Map<Paths, Map<Vec<Assertion>, Leaf>>,
}

impl Continuation {
    fn new(cached_assertions: Set<CachedAssertion>) -> Self {
        Continuation { cached_assertions, leaf_map: Map::new() }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Selector {
    pop_count: usize,
    index: usize,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Guard {
    Rec(Assertion, usize),
    Seq(usize),
}

#[derive(Debug)]
struct Leaf { // aka Topic
    cached_assertions: Set<CachedAssertion>,
    endpoints_map: Map<Paths, Endpoints>,
}

impl Leaf {
    fn new() -> Self {
        Leaf { cached_assertions: Set::new(), endpoints_map: Map::new() }
    }

    fn is_empty(&self) -> bool {
        self.cached_assertions.is_empty() && self.endpoints_map.is_empty()
    }
}

#[derive(Debug)]
struct Endpoints {
    cached_captures: Bag<Captures>,
    endpoints: Set<Endpoint>,
}

impl Endpoints {
    fn new(cached_captures: Bag<Captures>) -> Self {
        Endpoints { cached_captures, endpoints: Set::new() }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
enum CachedAssertion {
    VisibilityRestricted(Paths, Assertion),
    Unrestricted(Assertion),
}

impl CachedAssertion {
    fn unscope(&self) -> &Assertion {
        match self {
            CachedAssertion::VisibilityRestricted(_, a) => a,
            CachedAssertion::Unrestricted(a) => a,
        }
    }

    fn unpack(&self) -> (Option<&Paths>, &Assertion) {
        match self {
            CachedAssertion::VisibilityRestricted(ps, a) => (Some(ps), a),
            CachedAssertion::Unrestricted(a) => (None, a),
        }
    }
}

fn is_unrestricted(capture_paths: &Paths, restriction_paths: Option<&Paths>) -> bool {
    if restriction_paths.is_none() {
        return false;
    }

    panic!("Not yet implemented");
}
