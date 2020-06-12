use super::V;
use super::ConnId;
use super::packets::{self, Assertion, EndpointName, Captures};
use super::skeleton;

use preserves::value::{self, Map, NestedValue};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};
use tokio::sync::mpsc::UnboundedSender;

pub type DataspaceRef = Arc<RwLock<Dataspace>>;
pub type DataspaceError = (String, V);

#[derive(Debug)]
struct Actor {
    tx: UnboundedSender<packets::S2C>,
    queue_depth: Arc<AtomicUsize>,
    endpoints: Map<EndpointName, ActorEndpoint>,
}

#[derive(Debug)]
struct ActorEndpoint {
    analysis_results: Option<skeleton::AnalysisResults>,
    assertion: Assertion,
}

#[derive(Debug)]
pub struct Churn {
    pub peers_added: usize,
    pub peers_removed: usize,
    pub assertions_added: usize,
    pub assertions_removed: usize,
    pub endpoints_added: usize,
    pub endpoints_removed: usize,
    pub messages_injected: usize,
    pub messages_delivered: usize,
}

impl Churn {
    pub fn new() -> Self {
        Self {
            peers_added: 0,
            peers_removed: 0,
            assertions_added: 0,
            assertions_removed: 0,
            endpoints_added: 0,
            endpoints_removed: 0,
            messages_injected: 0,
            messages_delivered: 0,
        }
    }

    pub fn reset(&mut self) {
        self.peers_added = 0;
        self.peers_removed = 0;
        self.assertions_added = 0;
        self.assertions_removed = 0;
        self.endpoints_added = 0;
        self.endpoints_removed = 0;
        self.messages_injected = 0;
        self.messages_delivered = 0;
    }
}

#[derive(Debug)]
pub struct Dataspace {
    name: V,
    peers: Map<ConnId, Actor>,
    index: skeleton::Index,
    pub churn: Churn,
}

impl Dataspace {
    pub fn new(name: &V) -> Self {
        Self {
            name: name.clone(),
            peers: Map::new(),
            index: skeleton::Index::new(),
            churn: Churn::new(),
        }
    }

    pub fn new_ref(name: &V) -> DataspaceRef {
        Arc::new(RwLock::new(Self::new(name)))
    }

    pub fn register(&mut self, id: ConnId,
                    tx: UnboundedSender<packets::S2C>,
                    queue_depth: Arc<AtomicUsize>)
    {
        assert!(!self.peers.contains_key(&id));
        self.peers.insert(id, Actor {
            tx,
            queue_depth,
            endpoints: Map::new(),
        });
        self.churn.peers_added += 1;
    }

    pub fn deregister(&mut self, id: ConnId) {
        let ac = self.peers.remove(&id).unwrap();
        self.churn.peers_removed += 1;
        let mut outbound_turns: Map<ConnId, Vec<packets::Event>> = Map::new();
        for (epname, ep) in ac.endpoints {
            self.remove_endpoint(&mut outbound_turns, id, &epname, ep);
        }
        outbound_turns.remove(&id);
        self.deliver_outbound_turns(outbound_turns);
    }

    fn remove_endpoint(&mut self,
                       mut outbound_turns: &mut Map<ConnId, Vec<packets::Event>>,
                       id: ConnId,
                       epname: &EndpointName,
                       ep: ActorEndpoint)
    {
        let ActorEndpoint{ analysis_results, assertion } = ep;
        if let Some(ar) = analysis_results {
            self.index.remove_endpoint(&ar, skeleton::Endpoint {
                connection: id,
                name: epname.clone(),
            });
        }
        let old_assertions = self.index.assertion_count();
        schedule_events(&mut outbound_turns,
                        self.index.remove((&assertion).into()),
                        packets::Event::Del);
        self.churn.assertions_removed += old_assertions - self.index.assertion_count();
        self.churn.endpoints_removed += 1;
    }

    pub fn turn(&mut self, id: ConnId, actions: Vec<packets::Action>) ->
        Result<(), DataspaceError>
    {
        let mut outbound_turns: Map<ConnId, Vec<packets::Event>> = Map::new();
        for a in actions {
            tracing::trace!(action = debug(&a), "turn");
            match a {
                packets::Action::Assert(ref epname, ref assertion) => {
                    let ac = self.peers.get_mut(&id).unwrap();
                    if ac.endpoints.contains_key(&epname) {
                        return Err(("Duplicate endpoint name".to_string(), value::to_value(a)));
                    }

                    let ar =
                        if let Some(fs) = assertion.value().as_simple_record("observe", Some(1)) {
                            let ar = skeleton::analyze(&fs[0]);
                            let events = self.index.add_endpoint(&ar, skeleton::Endpoint {
                                connection: id,
                                name: epname.clone(),
                            });
                            outbound_turns.entry(id).or_insert_with(Vec::new).extend(events);
                            Some(ar)
                        } else {
                            None
                        };

                    let old_assertions = self.index.assertion_count();
                    schedule_events(&mut outbound_turns,
                                    self.index.insert(assertion.into()),
                                    packets::Event::Add);
                    self.churn.assertions_added += self.index.assertion_count() - old_assertions;
                    self.churn.endpoints_added += 1;

                    ac.endpoints.insert(epname.clone(), ActorEndpoint {
                        analysis_results: ar,
                        assertion: assertion.clone()
                    });
                }
                packets::Action::Clear(ref epname) => {
                    let ac = self.peers.get_mut(&id).unwrap();
                    match ac.endpoints.remove(epname) {
                        None => {
                            return Err(("Nonexistent endpoint name".to_string(), value::to_value(a)));
                        }
                        Some(ep) => {
                            self.remove_endpoint(&mut outbound_turns, id, epname, ep);
                            outbound_turns.entry(id).or_insert_with(Vec::new)
                                .push(packets::Event::End(epname.clone()));
                        }
                    }
                }
                packets::Action::Message(ref assertion) => {
                    schedule_events(&mut outbound_turns,
                                    self.index.send(assertion.into(),
                                                    &mut self.churn.messages_delivered),
                                    packets::Event::Msg);
                    self.churn.messages_injected += 1;
                }
            }
        }
        self.deliver_outbound_turns(outbound_turns);
        Ok(())
    }

    fn deliver_outbound_turns(&mut self, outbound_turns: Map<ConnId, Vec<packets::Event>>) {
        for (target, events) in outbound_turns {
            let actor = self.peers.get_mut(&target).unwrap();
            let _ = actor.tx.send(packets::S2C::Turn(events));
            actor.queue_depth.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn assertion_count(&self) -> usize {
        self.index.assertion_count()
    }

    pub fn endpoint_count(&self) -> isize {
        self.index.endpoint_count()
    }
}

fn schedule_events<C>(outbound_turns: &mut Map<ConnId, Vec<packets::Event>>,
                      events: skeleton::Events,
                      ctor: C)
where C: Fn(EndpointName, Captures) -> packets::Event
{
    for (eps, cs) in events {
        for ep in eps {
            outbound_turns.entry(ep.connection).or_insert_with(Vec::new)
                .push(ctor(ep.name, cs.clone()));
        }
    }
}
