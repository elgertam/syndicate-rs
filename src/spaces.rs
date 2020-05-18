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

    pub fn stats_string(&self, delta: core::time::Duration) -> String {
        let mut v = vec![];
        v.push(format!("{} dataspace(s)", self.index.len()));
        for (dsname, dsref) in &self.index {
            let mut ds = dsref.write().unwrap();
            v.push(format!("  {:?}: {} connection(s) {}, {} assertion(s) {}, {} endpoint(s) {}, msgs in {}/sec, out {}/sec",
                           dsname,
                           ds.peer_count(),
                           format!("(+{}/-{})", ds.churn.peers_added, ds.churn.peers_removed),
                           ds.assertion_count(),
                           format!("(+{}/-{})", ds.churn.assertions_added, ds.churn.assertions_removed),
                           ds.endpoint_count(),
                           format!("(+{}/-{})", ds.churn.endpoints_added, ds.churn.endpoints_removed),
                           ds.churn.messages_injected as f32 / delta.as_secs() as f32,
                           ds.churn.messages_delivered as f32 / delta.as_secs() as f32));
            ds.churn.reset();
        }
        v.join("\n")
    }
}
