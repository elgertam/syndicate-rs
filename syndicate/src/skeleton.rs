//! High-speed index over a set of assertions and a set of
//! [`Observe`rs][crate::schemas::dataspace::Observe] of those
//! assertions.
//!
//! Generally speaking, you will not need to use this module; instead,
//! create [`Dataspace`][crate::dataspace::Dataspace] entities.

use super::bag;

use preserves::CompoundClass;
use preserves::Map;
use preserves::Set;
use preserves::Value;
use preserves::ValueClass;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use crate::actor::AnyValue;
use crate::actor::Activation;
use crate::actor::Handle;
use crate::actor::Cap;
use crate::schemas::dataspace_patterns as ds;
use crate::pattern::{self, ConstantPositions, PathStep, Path, Paths};

type Bag<A> = bag::BTreeBag<A>;

type Captures = AnyValue;

/// Index of assertions and [`Observe`rs][crate::schemas::dataspace::Observe].
///
/// Generally speaking, you will not need to use this structure;
/// instead, create [`Dataspace`][crate::dataspace::Dataspace]
/// entities.
#[derive(Debug)]
pub struct Index {
    all_assertions: Bag<AnyValue>,
    observer_count: usize,
    root: Node,
}

#[derive(Debug)]
struct Node {
    continuation: Continuation,
    edges: Map<Selector, Map<ds::GroupType, Node>>,
}

#[derive(Debug)]
struct Continuation {
    cached_assertions: Set<AnyValue>,
    leaf_map: Map<Arc<ConstantPositions>, Map<Captures, Leaf>>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Selector {
    pop_count: usize,
    step: PathStep,
}

#[derive(Debug)]
struct Leaf { // aka Topic
    cached_assertions: Set<AnyValue>,
    endpoints_map: Map<Paths, Endpoints>,
}

#[derive(Debug)]
struct Endpoints {
    cached_captures: Bag<Captures>,
    endpoints: Map<Arc<Cap>, Map<Captures, Handle>>,
}

//---------------------------------------------------------------------------

impl Index {
    /// Construct a new `Index`.
    pub fn new() -> Self {
        Index {
            all_assertions: Bag::new(),
            observer_count: 0,
            root: Node::new(Continuation::new(Set::new())),
        }
    }

    /// Adds a new observer. If any existing assertions in the index
    /// match `pat`, establishes corresponding assertions at
    /// `observer`. Once the observer is registered, subsequent
    /// arriving assertions will be matched against `pat` and
    /// delivered to `observer` if they match.
    pub fn add_observer(
        &mut self,
        t: &mut Activation,
        pat: &ds::Pattern,
        observer: &Arc<Cap>,
    ) {
        let analysis = pattern::PatternAnalysis::new(pat);
        self.root.extend(pat).add_observer(t, &analysis, observer);
        self.observer_count += 1;
    }

    /// Removes an existing observer.
    pub fn remove_observer(
        &mut self,
        t: &mut Activation,
        pat: ds::Pattern,
        observer: &Arc<Cap>,
    ) {
        let analysis = pattern::PatternAnalysis::new(&pat);
        self.root.extend(&pat).remove_observer(t, analysis, observer);
        self.observer_count -= 1;
    }

    /// Inserts an assertion into the index, notifying matching observers.
    /// Answers `true` iff the assertion was new to the dataspace.
    pub fn insert(&mut self, t: &mut Activation, outer_value: &AnyValue) -> bool {
        let net = self.all_assertions.change(outer_value.clone(), 1);
        match net {
            bag::Net::AbsentToPresent => {
                Modification::new(
                    true,
                    &outer_value,
                    |c, v| { c.cached_assertions.insert(v.clone()); },
                    |l, v| { l.cached_assertions.insert(v.clone()); },
                    |es, cs| {
                        if es.cached_captures.change(cs.clone(), 1) == bag::Net::AbsentToPresent {
                            for (observer, capture_map) in &mut es.endpoints {
                                if let Some(h) = observer.assert(t, &(), &cs) {
                                    capture_map.insert(cs.clone(), h);
                                }
                            }
                        }
                    })
                    .perform(&mut self.root);
                true
            }
            bag::Net::PresentToPresent => false,
            _ => unreachable!(),
        }
    }

    /// Removes an assertion from the index, notifying matching observers.
    /// Answers `true` if it is the last in its equivalence class to be removed.
    pub fn remove(&mut self, t: &mut Activation, outer_value: &AnyValue) -> bool {
        let net = self.all_assertions.change(outer_value.clone(), -1);
        match net {
            bag::Net::PresentToAbsent => {
                Modification::new(
                    false,
                    &outer_value,
                    |c, v| { c.cached_assertions.remove(v); },
                    |l, v| { l.cached_assertions.remove(v); },
                    |es, cs| {
                        if es.cached_captures.change(cs.clone(), -1) == bag::Net::PresentToAbsent {
                            for capture_map in es.endpoints.values_mut() {
                                if let Some(h) = capture_map.remove(&cs) {
                                    t.retract(h);
                                }
                            }
                        }
                    })
                    .perform(&mut self.root);
                true
            }
            bag::Net::PresentToPresent => false,
            _ => unreachable!(),
        }
    }

    /// Routes a message using the index, notifying matching observers.
    pub fn send(&mut self, t: &mut Activation, outer_value: &AnyValue) {
        Modification::new(
            false,
            &outer_value,
            |_c, _v| (),
            |_l, _v| (),
            |es, cs| {
                // *delivery_count += es.endpoints.len();
                for observer in es.endpoints.keys() {
                    observer.message(t, &(), &cs);
                }
            }).perform(&mut self.root);
    }

    /// Retrieves the current count of distinct assertions in the index.
    pub fn assertion_count(&self) -> usize {
        return self.all_assertions.len()
    }

    /// Retrieves the current count of assertions in the index,
    /// including duplicates.
    pub fn endpoint_count(&self) -> isize {
        return self.all_assertions.total()
    }

    /// Retrieves the current count of observers of the index.
    pub fn observer_count(&self) -> usize {
        return self.observer_count
    }
}

impl Node {
    fn new(continuation: Continuation) -> Self {
        Node { continuation, edges: Map::new() }
    }

    fn extend(&mut self, pat: &ds::Pattern) -> &mut Continuation {
        let (_pop_count, final_node) = self.extend_walk(&mut Vec::new(), 0, PathStep::new(0), pat);
        &mut final_node.continuation
    }

    fn extend_walk(
        &mut self,
        path: &mut Path,
        pop_count: usize,
        step: PathStep,
        pat: &ds::Pattern,
    ) -> (usize, &mut Node) {
        let (guard, members): (ds::GroupType, Vec<(PathStep, &ds::Pattern)>) = match pat {
            ds::Pattern::Group { type_, entries } =>
                (type_.clone(),
                 entries.iter().map(|(k, p)| (k.clone(), p)).collect()),
            ds::Pattern::Bind { pattern } =>
                return self.extend_walk(path, pop_count, step, &**pattern),
            ds::Pattern::Discard | ds::Pattern::Lit { .. } =>
                return (pop_count, self),
        };

        let selector = Selector { pop_count, step };
        let continuation = &self.continuation;
        let table = self.edges.entry(selector).or_insert_with(Map::new);
        let mut next_node = table.entry(guard.clone()).or_insert_with(|| {
            Self::new(Continuation::new(
                continuation.cached_assertions.iter()
                    .filter(|a| match project_path((*a).clone(), path) {
                        Some(v) => Some(&guard) == class_of(&v).as_ref(),
                        None => false,
                    })
                    .cloned()
                    .collect()))
        });
        let mut pop_count = 0;
        for (step, kid) in members.into_iter() {
            path.push(step.clone());
            let (pc, nn) = next_node.extend_walk(path, pop_count, step, kid);
            pop_count = pc;
            next_node = nn;
            path.pop();
        }
        (pop_count + 1, next_node)
    }
}

#[derive(Debug)]
enum Stack<'a, T> {
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
where FCont: FnMut(&mut Continuation, &AnyValue) -> (),
      FLeaf: FnMut(&mut Leaf, &AnyValue) -> (),
      FEndpoints: FnMut(&mut Endpoints, Captures) -> ()
{
    create_leaf_if_absent: bool,
    outer_value: &'op AnyValue,
    m_cont: FCont,
    m_leaf: FLeaf,
    m_endpoints: FEndpoints,
}

impl<'op, FCont, FLeaf, FEndpoints> Modification<'op, FCont, FLeaf, FEndpoints>
where FCont: FnMut(&mut Continuation, &AnyValue) -> (),
      FLeaf: FnMut(&mut Leaf, &AnyValue) -> (),
      FEndpoints: FnMut(&mut Endpoints, Captures) -> ()
{
    fn new(create_leaf_if_absent: bool,
           outer_value: &'op AnyValue,
           m_cont: FCont,
           m_leaf: FLeaf,
           m_endpoints: FEndpoints,
    ) -> Self {
        Modification {
            create_leaf_if_absent,
            outer_value,
            m_cont,
            m_leaf,
            m_endpoints,
        }
    }

    fn perform(&mut self, n: &mut Node) {
        self.node(n, &Stack::Item(&Value::new(vec![self.outer_value.clone()]), &Stack::Empty))
    }

    fn node(&mut self, n: &mut Node, term_stack: &Stack<&AnyValue>) {
        self.continuation(&mut n.continuation);
        for (selector, table) in &mut n.edges {
            let mut next_stack = term_stack;
            for _ in 0..selector.pop_count { next_stack = next_stack.pop() }
            if let Some(next_value) = step(next_stack.top(), &selector.step) {
                if let Some(next_class) = class_of(&next_value) {
                    if let Some(next_node) = table.get_mut(&next_class) {
                        self.node(next_node, &Stack::Item(&next_value, next_stack))
                    }
                }
            }
        }
    }

    fn continuation(&mut self, c: &mut Continuation) {
        (self.m_cont)(c, self.outer_value);
        let mut empty_const_positions = Vec::new();
        for (const_positions, const_val_map) in &mut c.leaf_map {
            if project_paths(self.outer_value, &const_positions.required_to_exist).is_none() {
                continue;
            }
            let const_vals = match project_paths(self.outer_value, &const_positions.with_values) {
                Some(vs) => vs,
                None => continue,
            };
            let leaf_opt = if self.create_leaf_if_absent {
                Some(const_val_map.entry(const_vals.clone()).or_insert_with(Leaf::new))
            } else {
                const_val_map.get_mut(&const_vals)
            };
            if let Some(leaf) = leaf_opt {
                (self.m_leaf)(leaf, self.outer_value);
                for (capture_paths, endpoints) in &mut leaf.endpoints_map {
                    if let Some(cs) = project_paths(self.outer_value, &capture_paths) {
                        (self.m_endpoints)(endpoints, cs);
                    }
                }
                if leaf.is_empty() {
                    const_val_map.remove(&const_vals);
                    if const_val_map.is_empty() {
                        empty_const_positions.push(const_positions.clone());
                    }
                }
            }
        }
        for const_positions in empty_const_positions {
            c.leaf_map.remove(&const_positions);
        }
    }
}

fn class_of(v: &AnyValue) -> Option<ds::GroupType> {
    match v.value_class() {
        ValueClass::Compound(CompoundClass::Sequence) => Some(ds::GroupType::Arr),
        ValueClass::Compound(CompoundClass::Record) => Some(ds::GroupType::Rec { label: v.label() }),
        ValueClass::Compound(CompoundClass::Dictionary) => Some(ds::GroupType::Dict),
        _ => None,
    }
}

fn project_path<'a>(mut v: AnyValue, p: &Path) -> Option<AnyValue> {
    for i in p {
        match step(&v, i) {
            Some(w) => v = w,
            None => return None,
        }
    }
    Some(v)
}

fn project_paths<'a>(v: &'a AnyValue, ps: &Paths) -> Option<Captures> {
    let mut vs = Vec::new();
    for p in ps {
        match project_path(v.clone(), p) {
            Some(c) => vs.push(c.clone()),
            None => return None,
        }
    }
    Some(Captures::new(vs))
}

fn step<'a>(v: &'a AnyValue, s: &PathStep) -> Option<AnyValue> {
    match v.value_class() {
        ValueClass::Compound(CompoundClass::Sequence) => {
            let i = s.as_usize()?.ok()?;
            if i < v.len() { Some(v.index(i)) } else { None }
        }
        ValueClass::Compound(CompoundClass::Record) => {
            let i = s.as_usize()?.ok()?;
            if i < v.len() { Some(v.index(i)) } else { None }
        }
        ValueClass::Compound(CompoundClass::Dictionary) => v.get(s),
        _ => None,
    }
}

impl Continuation {
    fn new(cached_assertions: Set<AnyValue>) -> Self {
        Continuation { cached_assertions, leaf_map: Map::new() }
    }

    fn add_observer(
        &mut self,
        t: &mut Activation,
        analysis: &pattern::PatternAnalysis,
        observer: &Arc<Cap>,
    ) {
        let cached_assertions = &self.cached_assertions;
        let const_val_map =
            self.leaf_map.entry(analysis.const_positions.clone()).or_insert_with({
                || {
                    let mut cvm = Map::new();
                    for a in cached_assertions {
                        if project_paths(a, &analysis.const_positions.required_to_exist).is_none() {
                            continue;
                        }
                        if let Some(key) = project_paths(a, &analysis.const_positions.with_values) {
                            cvm.entry(key).or_insert_with(Leaf::new)
                                .cached_assertions.insert(a.clone());
                        }
                    }
                    cvm
                }
            });
        let leaf = const_val_map.entry(analysis.const_values.clone()).or_insert_with(Leaf::new);
        let leaf_cached_assertions = &leaf.cached_assertions;
        let endpoints = leaf.endpoints_map.entry(analysis.capture_paths.clone()).or_insert_with(|| {
            let mut b = Bag::new();
            for term in leaf_cached_assertions {
                if let Some(captures) = project_paths(term, &analysis.capture_paths) {
                    *b.entry(captures).or_insert(0) += 1;
                }
            }
            Endpoints { cached_captures: b, endpoints: Map::new() }
        });
        let mut capture_map = Map::new();
        for cs in endpoints.cached_captures.keys() {
            if let Some(h) = observer.assert(t, &(), cs) {
                capture_map.insert(cs.clone(), h);
            }
        }
        endpoints.endpoints.insert(observer.clone(), capture_map);
    }

    fn remove_observer(
        &mut self,
        t: &mut Activation,
        analysis: pattern::PatternAnalysis,
        observer: &Arc<Cap>,
    ) {
        if let Entry::Occupied(mut const_val_map_entry)
            = self.leaf_map.entry(analysis.const_positions)
        {
            let const_val_map = const_val_map_entry.get_mut();
            if let Entry::Occupied(mut leaf_entry)
                = const_val_map.entry(analysis.const_values)
            {
                let leaf = leaf_entry.get_mut();
                if let Entry::Occupied(mut endpoints_entry)
                    = leaf.endpoints_map.entry(analysis.capture_paths)
                {
                    let endpoints = endpoints_entry.get_mut();
                    if let Some(capture_map) = endpoints.endpoints.remove(observer) {
                        for handle in capture_map.into_values() {
                            t.retract(handle)
                        }
                    }
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

impl Leaf {
    fn new() -> Self {
        Leaf { cached_assertions: Set::new(), endpoints_map: Map::new() }
    }

    fn is_empty(&self) -> bool {
        self.cached_assertions.is_empty() && self.endpoints_map.is_empty()
    }
}
