use preserves::Map;
use preserves::Set;

use std::fmt::Debug;

#[derive(Debug)]
pub struct Graph<ObjectId: Debug + Clone + Eq + Ord, SubjectId: Debug + Clone + Eq + Ord>
{
    forward_edges: Map<ObjectId, Set<SubjectId>>,
    reverse_edges: Map<SubjectId, Set<ObjectId>>,
    damaged_nodes: Set<ObjectId>,
}

impl<ObjectId: Debug + Clone + Eq + Ord, SubjectId: Debug + Clone + Eq + Ord>
    Graph<ObjectId, SubjectId>
{
    pub fn new() -> Self {
        Graph {
            forward_edges: Map::new(),
            reverse_edges: Map::new(),
            damaged_nodes: Set::new(),
        }
    }

    pub fn is_clean(&self) -> bool {
        self.damaged_nodes.is_empty()
    }

    pub fn record_observation(&mut self, observer: SubjectId, observed: ObjectId) {
        self.forward_edges.entry(observed.clone()).or_default().insert(observer.clone());
        self.reverse_edges.entry(observer).or_default().insert(observed);
    }

    pub fn record_damage(&mut self, observed: ObjectId) {
        self.damaged_nodes.insert(observed);
    }

    fn forget_subject(&mut self, observer: &SubjectId) {
        if let Some(observeds) = self.reverse_edges.remove(observer) {
            for observed in observeds.into_iter() {
                if let Some(observers) = self.forward_edges.get_mut(&observed) {
                    observers.remove(observer);
                }
            }
        }
    }

    // To repair: repeat:
    //  - take_damaged_nodes; if none, return successfully - otherwise:
    //    - for each, take_observers_of.
    //      - for each, invoke the observer's repair function.

    pub fn take_damaged_nodes(&mut self) -> Set<ObjectId> {
        std::mem::take(&mut self.damaged_nodes)
    }

    pub fn take_observers_of(&mut self, observed: &ObjectId) -> Set<SubjectId> {
        let observers = self.forward_edges.remove(&observed).unwrap_or_default();
        for observer in observers.iter() {
            self.forget_subject(observer);
        }
        observers
    }
}
