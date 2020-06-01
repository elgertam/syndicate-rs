use super::V;
use super::dataspace;

use std::sync::Arc;

use tracing::{info, debug};

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

        debug!(name = debug(name),
               action = display(if is_new { "created" } else { "accessed" }));

        space
    }

    pub fn cleanup(&mut self) {
        self.index = self.index.iter()
            .filter(|s| s.1.read().unwrap().peer_count() > 0)
            .map(|(k,v)| (k.clone(), Arc::clone(v)))
            .collect();
    }

    pub fn dump_stats(&self, delta: core::time::Duration)  {
        for (dsname, dsref) in &self.index {
            let mut ds = dsref.write().unwrap();
            info!(name = debug(dsname),
                  connections = display(format!("{} (+{}/-{})", ds.peer_count(), ds.churn.peers_added, ds.churn.peers_removed)),
                  assertions = display(format!("{} (+{}/-{})", ds.assertion_count(), ds.churn.assertions_added, ds.churn.assertions_removed)),
                  endpoints = display(format!("{} (+{}/-{})", ds.endpoint_count(), ds.churn.endpoints_added, ds.churn.endpoints_removed)),
                  msg_in_rate = display(ds.churn.messages_injected as f32 / delta.as_secs() as f32),
                  msg_out_rate = display(ds.churn.messages_delivered as f32 / delta.as_secs() as f32));
            ds.churn.reset();
        }
    }
}
