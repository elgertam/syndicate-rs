// use std::sync::Arc;
use std::collections::HashMap;
use std::collections::HashSet;

use super::bag::HashBag;
use super::term::Term;

pub enum Event {
    Removed,
    Message,
    Added,
}

type Path = Vec<usize>;
type Paths = Vec<Path>;
type Captures<'a> = Vec<Term<'a>>;

type Callback<'a> = FnMut(Event, Captures<'a>) -> ();

pub enum Skeleton<'a> {
    Blank,
    Guarded(Guard<'a>, Vec<Skeleton<'a>>)
}

pub struct AnalysisResults<'a> {
    skeleton: Skeleton<'a>,
    constPaths: Paths,
    constVals: Vec<Term<'a>>,
    capturePaths: Captures<'a>,
    assertion: Term<'a>,
}

pub struct Index<'a> {
    all_assertions: HashBag<Term<'a>>,
}

impl<'a> Index<'a> {
    pub fn new() -> Self {
        Index {
            all_assertions: HashBag::new(),
        }
    }

    pub fn add_handler(analysis_results: AnalysisResults, 
}

struct Node<'a> {
    continuation: Continuation<'a>,
    edges: HashMap<Selector, HashMap<Guard<'a>, Node<'a>>>,
}

struct Continuation<'a> {
    cached_assertions: HashSet<Term<'a>>,
    leaf_map: HashMap<Paths, HashMap<Vec<Term<'a>>, Leaf<'a>>>,
}

struct Selector {
    pop_count: usize,
    index: usize,
}

enum Guard<'a> {
    Rec(&'a Term<'a>, usize),
    Seq(usize),
}

struct Leaf<'a> { // aka Topic
    cached_assertions: HashSet<Term<'a>>,
    handler_map: HashMap<Paths, Handler<'a>>,
}

struct Handler<'a> {
    cached_captures: HashBag<Captures<'a>>,
    callbacks: HashSet<&'a Callback<'a>>,
}
