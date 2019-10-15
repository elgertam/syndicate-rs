// pub type V = preserves::value::ArcValue;

// type Map<A,B> = std::collections::BTreeMap<A,B>;
// type Set<A> = std::collections::BTreeSet<A>;
// type Bag<A> = super::bag::BTreeBag<A>;

// pub enum Event {
//     Removed,
//     Message,
//     Added,
// }

// type Path = Vec<usize>;
// type Paths = Vec<Path>;
// type Captures = Vec<V>;

// trait HandleEvent {
//     fn handle_event<'a>(self, captures: &Captures);
// }

// pub enum Skeleton {
//     Blank,
//     Guarded(Guard, Vec<Skeleton>)
// }

// pub struct AnalysisResults {
//     skeleton: Skeleton,
//     const_paths: Paths,
//     const_vals: Vec<V>,
//     capture_paths: Paths,
//     assertion: V,
// }

// pub struct Index {
//     all_assertions: Bag<V>,
// }

// impl Index {
//     pub fn new() -> Self {
//         Index{ all_assertions: Bag::new() }
//     }

//     // pub fn add_handler(analysis_results: AnalysisResults, 
// }

// struct Node {
//     continuation: Continuation,
//     edges: Map<Selector, Map<Guard, Node>>,
// }

// struct Continuation {
//     cached_assertions: Set<V>,
//     leaf_map: Map<Paths, Map<Vec<V>, Leaf>>,
// }

// struct Selector {
//     pop_count: usize,
//     index: usize,
// }

// pub enum Guard {
//     Rec(V, usize),
//     Seq(usize),
// }

// struct Leaf { // aka Topic
//     cached_assertions: Set<V>,
//     handler_map: Map<Paths, Handler>,
// }

// struct Handler {
//     cached_captures: Bag<Captures>,
//     callbacks: Set<Box<dyn HandleEvent>>,
// }
