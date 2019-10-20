use super::V;
use super::ConnId;
use super::packets;

use preserves::value::Map;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::UnboundedSender;

pub type DataspaceRef = Arc<RwLock<Dataspace>>;

pub enum DataspaceError {
}

impl From<DataspaceError> for std::io::Error {
    fn from(v: DataspaceError) -> Self {
        panic!("No DataspaceErrors to convert!")
    }
}

#[derive(Debug)]
pub struct Dataspace {
    name: V,
    peers: Map<ConnId, UnboundedSender<packets::Out>>,
}

impl Dataspace {
    pub fn new(name: &V) -> Self {
        Self { name: name.clone(), peers: Map::new() }
    }

    pub fn new_ref(name: &V) -> DataspaceRef {
        Arc::new(RwLock::new(Self::new(name)))
    }

    pub fn register(&mut self, id: ConnId, tx: UnboundedSender<packets::Out>) {
        assert!(!self.peers.contains_key(&id));
        self.peers.insert(id, tx);
    }

    pub fn deregister(&mut self, id: ConnId) {
        self.peers.remove(&id);
    }

    pub fn turn(&mut self, actions: Vec<packets::Action>) -> Result<(), DataspaceError> {
        for a in actions {
            println!("Turn action: {:?}", &a);
        }
        Ok(())
    }
}
