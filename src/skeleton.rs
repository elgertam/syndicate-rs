use super::ConnId;
use super::Syndicate;
use super::bag;
use super::packets::Assertion;
use super::packets::Captures;
use super::packets::EndpointName;
use super::packets::Event;

use preserves::value::{Map, Set, Value, NestedValue};
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::iter::FromIterator;
use std::sync::Arc;

type Bag<A> = bag::BTreeBag<A>;

pub type Path = Vec<usize>;
pub type Paths = Vec<Path>;
pub type Events = Vec<(Vec<Endpoint>, Captures)>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Endpoint {
    pub connection: ConnId,
    pub name: EndpointName,
}

#[derive(Debug)]
pub enum Skeleton {
    Blank,
    Guarded(Guard, Vec<Skeleton>)
}

#[derive(Debug)]
pub struct AnalysisResults {
    pub skeleton: Skeleton,
    pub const_paths: Paths,
    pub const_vals: Captures,
    pub capture_paths: Paths,
    pub assertion: Assertion,
}

#[derive(Debug)]
pub struct Index {
    all_assertions: Bag<CachedAssertion>,
    root: Node,
}

impl Index {
    pub fn new() -> Self {
        Index{ all_assertions: Bag::new(), root: Node::new(Continuation::new(Set::new())) }
    }

    pub fn add_endpoint(&mut self, analysis_results: &AnalysisResults, endpoint: Endpoint)
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
        let capture_paths = &analysis_results.capture_paths;
        let leaf = const_val_map.entry(analysis_results.const_vals.clone()).or_insert_with(Leaf::new);
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

    pub fn remove_endpoint(&mut self, analysis_results: &AnalysisResults, endpoint: Endpoint) {
        let continuation = self.root.extend(&analysis_results.skeleton);
        if let Entry::Occupied(mut const_val_map_entry)
            = continuation.leaf_map.entry(analysis_results.const_paths.clone())
        {
            let const_val_map = const_val_map_entry.get_mut();
            if let Entry::Occupied(mut leaf_entry)
                = const_val_map.entry(analysis_results.const_vals.clone())
            {
                let leaf = leaf_entry.get_mut();
                if let Entry::Occupied(mut endpoints_entry)
                    = leaf.endpoints_map.entry(analysis_results.capture_paths.clone())
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

    pub fn insert(&mut self, v: CachedAssertion) -> Events {
        self.adjust(v, 1)
    }

    pub fn remove(&mut self, v: CachedAssertion) -> Events {
        self.adjust(v, -1)
    }

    pub fn adjust(&mut self, outer_value: CachedAssertion, delta: bag::Count) -> Events {
        let mut outputs = Vec::new();
        let net = self.all_assertions.change(outer_value.clone(), delta);
        match net {
            bag::Net::AbsentToPresent => {
                Modification::new(
                    true,
                    &outer_value,
                    |c, v| { c.cached_assertions.insert(v.clone()); },
                    |l, v| { l.cached_assertions.insert(v.clone()); },
                    |es, cs| {
                        if es.cached_captures.change(cs.clone(), 1) == bag::Net::AbsentToPresent {
                            outputs.push((es.endpoints.iter().cloned().collect(), cs))
                        }
                    })
                    .perform(&mut self.root);
            }
            bag::Net::PresentToAbsent => {
                Modification::new(
                    false,
                    &outer_value,
                    |c, v| { c.cached_assertions.remove(v); },
                    |l, v| { l.cached_assertions.remove(v); },
                    |es, cs| {
                        if es.cached_captures.change(cs.clone(), -1) == bag::Net::PresentToAbsent {
                            outputs.push((es.endpoints.iter().cloned().collect(), cs))
                        }
                    })
                    .perform(&mut self.root);
            }
            _ => ()
        }
        outputs
    }

    pub fn send(&mut self, outer_value: CachedAssertion) -> Events {
        let mut outputs = Vec::new();
        Modification::new(
            false,
            &outer_value,
            |_c, _v| (),
            |_l, _v| (),
            |es, cs| outputs.push((es.endpoints.iter().cloned().collect(), cs)))
            .perform(&mut self.root);
        outputs
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

#[derive(Debug)]
pub enum Stack<'a, T> {
    Empty,
    Item(T, &'a Stack<'a, T>)
}

impl<'a, T> Stack<'a, T> {
    fn pop(&self) -> &Self {
        match self {
            Stack::Empty => panic!("Internal error: pop: Incorrect pop_count computation"),
            Stack::Item(_, tail) => tail
        }
    }

    fn top(&self) -> &T {
        match self {
            Stack::Empty => panic!("Internal error: top: Incorrect pop_count computation"),
            Stack::Item(item, _) => item
        }
    }
}

struct Modification<'op, FCont, FLeaf, FEndpoints>
where FCont: FnMut(&mut Continuation, &CachedAssertion) -> (),
      FLeaf: FnMut(&mut Leaf, &CachedAssertion) -> (),
      FEndpoints: FnMut(&mut Endpoints, Captures) -> ()
{
    create_leaf_if_absent: bool,
    outer_value: &'op CachedAssertion,
    restriction_paths: Option<&'op Paths>,
    outer_value_term: &'op Assertion,
    m_cont: FCont,
    m_leaf: FLeaf,
    m_endpoints: FEndpoints,
}

impl<'op, FCont, FLeaf, FEndpoints> Modification<'op, FCont, FLeaf, FEndpoints>
where FCont: FnMut(&mut Continuation, &CachedAssertion) -> (),
      FLeaf: FnMut(&mut Leaf, &CachedAssertion) -> (),
      FEndpoints: FnMut(&mut Endpoints, Captures) -> ()
{
    fn new(create_leaf_if_absent: bool,
           outer_value: &'op CachedAssertion,
           m_cont: FCont,
           m_leaf: FLeaf,
           m_endpoints: FEndpoints) -> Self {
        let (restriction_paths, outer_value_term) = outer_value.unpack();
        Modification {
            create_leaf_if_absent,
            outer_value,
            restriction_paths,
            outer_value_term,
            m_cont,
            m_leaf,
            m_endpoints,
        }
    }

    fn perform(&mut self, n: &mut Node) {
        self.node(n, &Stack::Item(&Value::from(vec![self.outer_value_term.clone()]).wrap(), &Stack::Empty))
    }

    fn node(&mut self, n: &mut Node, term_stack: &Stack<&Assertion>) {
        self.continuation(&mut n.continuation);
        for (selector, table) in &mut n.edges {
            let mut next_stack = term_stack;
            for _ in 0..selector.pop_count { next_stack = next_stack.pop() }
            let next_value = step(next_stack.top(), selector.index);
            if let Some(next_class) = class_of(next_value) {
                if let Some(next_node) = table.get_mut(&next_class) {
                    self.node(next_node, &Stack::Item(next_value, next_stack))
                }
            }
        }
    }

    fn continuation(&mut self, c: &mut Continuation) {
        (self.m_cont)(c, self.outer_value);
        let mut empty_const_paths = Vec::new();
        for (const_paths, const_val_map) in &mut c.leaf_map {
            let const_vals = project_paths(self.outer_value_term, const_paths);
            let leaf_opt = if self.create_leaf_if_absent {
                Some(const_val_map.entry(const_vals.clone()).or_insert_with(Leaf::new))
            } else {
                const_val_map.get_mut(&const_vals)
            };
            if let Some(leaf) = leaf_opt {
                (self.m_leaf)(leaf, self.outer_value);
                for (capture_paths, endpoints) in &mut leaf.endpoints_map {
                    if is_unrestricted(&capture_paths, self.restriction_paths) {
                        (self.m_endpoints)(endpoints,
                                           project_paths(self.outer_value_term, &capture_paths));
                    }
                }
                if leaf.is_empty() {
                    const_val_map.remove(&const_vals);
                    if const_val_map.is_empty() {
                        empty_const_paths.push(const_paths.clone());
                    }
                }
            }
        }
        for const_paths in empty_const_paths {
            c.leaf_map.remove(&const_paths);
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

fn project_paths<'a>(v: &'a Assertion, ps: &Paths) -> Captures {
    Arc::new(ps.iter().map(|p| project_path(v, p)).cloned().collect())
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
    leaf_map: Map<Paths, Map<Captures, Leaf>>,
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

impl Guard {
    fn arity(&self) -> usize {
        match self {
            Guard::Rec(_, s) => *s,
            Guard::Seq(s) => *s
        }
    }
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
pub enum CachedAssertion {
    VisibilityRestricted(Paths, Assertion),
    Unrestricted(Assertion),
}

impl From<&Assertion> for CachedAssertion {
    fn from(a: &Assertion) -> Self {
        CachedAssertion::Unrestricted(a.clone())
    }
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
    // We are "unrestricted" if Set(capture_paths) ⊆ Set(restriction_paths). Since both
    // variables really hold lists, we operate with awareness of the order the lists are
    // built here. We know that the lists are built in fringe order; that is, they are
    // sorted wrt `pathCmp`.
    match restriction_paths {
        None => true, // not visibility-restricted in the first place
        Some(rpaths) => {
            let mut rpi = rpaths.iter();
            'outer: for c in capture_paths {
                'inner: loop {
                    match rpi.next() {
                        None => {
                            // there's at least one capture_paths entry (`c`) that does
                            // not appear in restriction_paths, so we are restricted
                            return false;
                        }
                        Some(r) => match c.cmp(r) {
                            Ordering::Less => {
                                // `c` is less than `r`, but restriction_paths is sorted,
                                // so `c` does not appear in restriction_paths, and we are
                                // thus restricted.
                                return false;
                            }
                            Ordering::Equal => {
                                // `c` is equal to `r`, so we may yet be unrestricted.
                                // Discard both `c` and `r` and continue.
                                continue 'outer;
                            }
                            Ordering::Greater => {
                                // `c` is greater than `r`, but capture_paths and
                                // restriction_paths are sorted, so while we might yet
                                // come to an `r` that is equal to `c`, we will never find
                                // another `c` that is less than this `c`. Discard this
                                // `r` then, keeping the `c`, and compare against the next
                                // `r`.
                                continue 'inner;
                            }
                        }
                    }
                }
            }
            // We went all the way through capture_paths without finding any `c` not in
            // restriction_paths.
            true
        }
    }
}

pub struct Analyzer {
    const_paths: Paths,
    const_vals: Vec<Assertion>,
    capture_paths: Paths,
    path: Path,
}

impl Analyzer {
    fn walk(&mut self, mut a: &Assertion) -> Skeleton {
        while let Some(fields) = a.value().as_simple_record("Capture", Some(1)) {
            self.capture_paths.push(self.path.clone());
            a = &fields[0];
        }

        if a.value().is_simple_record("Discard", Some(0)) {
            Skeleton::Blank
        } else {
            match class_of(a) {
                Some(cls) => {
                    let arity = cls.arity();
                    Skeleton::Guarded(cls,
                                      (0..arity).map(|i| {
                                          self.path.push(i);
                                          let s = self.walk(step(a, i));
                                          self.path.pop();
                                          s
                                      }).collect())
                }
                None => {
                    self.const_paths.push(self.path.clone());
                    self.const_vals.push(a.clone());
                    Skeleton::Blank
                }
            }
        }
    }
}

pub fn analyze(a: &Assertion) -> AnalysisResults {
    let mut z = Analyzer {
        const_paths: Vec::new(),
        const_vals: Vec::new(),
        capture_paths: Vec::new(),
        path: Vec::new(),
    };
    let skeleton = z.walk(a);
    AnalysisResults {
        skeleton,
        const_paths: z.const_paths,
        const_vals: Arc::new(z.const_vals),
        capture_paths: z.capture_paths,
        assertion: a.clone(),
    }
}

pub fn instantiate_assertion(a: &Assertion, cs: Captures) -> CachedAssertion {
    let mut capture_paths = Vec::new();
    let mut path = Vec::new();
    let mut vs: Vec<Assertion> = (*cs).clone();
    vs.reverse();
    let instantiated = instantiate_assertion_walk(&mut capture_paths, &mut path, &mut vs, a);
    CachedAssertion::VisibilityRestricted(capture_paths, instantiated)
}

fn instantiate_assertion_walk(capture_paths: &mut Paths,
                              path: &mut Path,
                              vs: &mut Vec<Assertion>,
                              a: &Assertion) -> Assertion {
    if let Some(fields) = a.value().as_simple_record("Capture", Some(1)) {
        capture_paths.push(path.clone());
        let v = vs.pop().unwrap();
        instantiate_assertion_walk(capture_paths, path, vs, &fields[0]);
        v
    } else if a.value().is_simple_record("Discard", Some(0)) {
        Value::Domain(Syndicate::new_placeholder()).wrap()
    } else {
        let f = |(i, aa)| {
            path.push(i);
            let vv = instantiate_assertion_walk(capture_paths,
                                                path,
                                                vs,
                                                aa);
            path.pop();
            vv
        };
        match class_of(a) {
            Some(Guard::Seq(_)) =>
                Value::from(Vec::from_iter(a.value().as_sequence().unwrap()
                                           .iter().enumerate().map(f)))
                .wrap(),
            Some(Guard::Rec(l, _)) =>
                Value::record(l, a.value().as_record().unwrap().1
                              .iter().enumerate().map(f).collect())
                .wrap(),
            None =>
                a.clone(),
        }
    }
}
