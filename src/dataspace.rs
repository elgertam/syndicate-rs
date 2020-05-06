use super::V;
use super::ConnId;
use super::packets::{self, Assertion, EndpointName, Captures};
use super::skeleton;

use preserves::value::{self, Map, NestedValue};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::UnboundedSender;

pub type DataspaceRef = Arc<RwLock<Dataspace>>;
pub type DataspaceError = (String, V);

#[derive(Debug)]
struct Actor {
    tx: UnboundedSender<packets::Out>,
    endpoints: Map<EndpointName, ActorEndpoint>,
}

#[derive(Debug)]
struct ActorEndpoint {
    analysis_results: Option<skeleton::AnalysisResults>,
    assertion: Assertion,
}

#[derive(Debug)]
pub struct Dataspace {
    name: V,
    peers: Map<ConnId, Actor>,
    index: skeleton::Index,
}

impl Dataspace {
    pub fn new(name: &V) -> Self {
        Self { name: name.clone(), peers: Map::new(), index: skeleton::Index::new() }
    }

    pub fn new_ref(name: &V) -> DataspaceRef {
        Arc::new(RwLock::new(Self::new(name)))
    }

    pub fn register(&mut self, id: ConnId, tx: UnboundedSender<packets::Out>) {
        assert!(!self.peers.contains_key(&id));
        self.peers.insert(id, Actor {
            tx,
            endpoints: Map::new(),
        });
    }

    pub fn deregister(&mut self, id: ConnId) {
        let ac = self.peers.remove(&id).unwrap();
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
        schedule_events(&mut outbound_turns,
                        self.index.remove((&assertion).into()),
                        |epname, cs| packets::Event::Del(epname, cs));
    }

    pub fn turn(&mut self, id: ConnId, actions: Vec<packets::Action>) ->
        Result<(), DataspaceError>
    {
        let mut outbound_turns: Map<ConnId, Vec<packets::Event>> = Map::new();
        for a in actions {
            println!("Turn action: {:?}", &a);
            match a {
                packets::Action::Assert(ref epname, ref assertion) => {
                    let ac = self.peers.get_mut(&id).unwrap();
                    if ac.endpoints.contains_key(&epname) {
                        return Err(("Duplicate endpoint name".to_string(),
                                    value::to_value(a).unwrap()));
                    }

                    let ar =
                        if let Some(fs) = assertion.value().as_simple_record("Observe", Some(1)) {
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

                    schedule_events(&mut outbound_turns,
                                    self.index.insert(assertion.into()),
                                    |epname, cs| packets::Event::Add(epname, cs));

                    ac.endpoints.insert(epname.clone(), ActorEndpoint {
                        analysis_results: ar,
                        assertion: assertion.clone()
                    });
                }
                packets::Action::Clear(ref epname) => {
                    let ac = self.peers.get_mut(&id).unwrap();
                    match ac.endpoints.remove(epname) {
                        None => {
                            return Err(("Nonexistent endpoint name".to_string(),
                                        value::to_value(a).unwrap()));
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
                                    self.index.send(assertion.into()),
                                    |epname, cs| packets::Event::Msg(epname, cs));
                }
            }
        }
        self.deliver_outbound_turns(outbound_turns);
        Ok(())
    }

    fn deliver_outbound_turns(&mut self, outbound_turns: Map<ConnId, Vec<packets::Event>>) {
        for (target, events) in outbound_turns {
            let _ = self.peers.get_mut(&target).unwrap().tx.send(packets::Out::Turn(events));
        }
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
