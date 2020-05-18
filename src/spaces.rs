use super::V;
use super::dataspace;

use std::sync::Arc;

use preserves::value::Map;

pub struct Spaces {
    index: Map<V, dataspace::DataspaceRef>,
}

impl Spaces {
    pub fn new() -> Self {
        Self { index: Map::new() }
    }

    pub fn lookup(&mut self, name: &V) -> dataspace::DataspaceRef {
        let (is_new, space) = match self.index.get(name) {
            Some(s) => (false, s.clone()),
            None => {
                let s = dataspace::Dataspace::new_ref(name);
                self.index.insert(name.clone(), s.clone());
                (true, s)
            }
        };

        println!("Dataspace {:?} {}",
                 name,
                 if is_new { "created" } else { "accessed" });

        space
    }

    pub fn cleanup(&mut self) {
        self.index = self.index.iter()
            .filter(|s| s.1.read().unwrap().peer_count() > 0)
            .map(|(k,v)| (k.clone(), Arc::clone(v)))
            .collect();
    }

    pub fn summary_string(&self) -> String {
        let mut v = vec![];
        v.push(format!("{} dataspace(s)", self.index.len()));
        for (dsname, dsref) in &self.index {
            let ds = dsref.read().unwrap();
            v.push(format!("  {:?}: {} connection(s), {} assertion(s), {} endpoint(s)",
                           dsname,
                           ds.peer_count(),
                           ds.assertion_count(),
                           ds.endpoint_count()));
        }
        v.join("\n")
    }
}
