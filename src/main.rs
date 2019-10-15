#![recursion_limit="256"]

mod bag;
mod skeleton;

use bytes::BytesMut;
use preserves::value::{self, Map};
use tokio::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::codec::{Framed, Encoder, Decoder};
use futures::select;

// use self::skeleton::Index;

type ConnId = u64;

// type V = value::PlainValue;
type V = value::ArcValue;

mod packets {
    use super::V;

    pub type EndpointName = V;
    pub type Assertion = V;
    pub type Captures = Vec<Assertion>;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub enum Action {
        Assert(EndpointName, Assertion),
        Clear(EndpointName),
        Message(Assertion),
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub enum Event {
        Add(EndpointName, Captures),
        Del(EndpointName, Captures),
        Msg(EndpointName, Captures),
        End(EndpointName),
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub enum In {
        Connect(V),
        Turn(Vec<Action>),
        Ping(),
        Pong(),
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub enum Out {
        Err(String),
        Turn(Vec<Event>),
        Ping(),
        Pong(),
    }
}

#[derive(Debug)]
pub enum DataspaceMessage {
    Join(ConnId, Sender<packets::Out>),
    Input(ConnId, packets::In),
    Leave(ConnId),
}

struct ValueCodec {
    codec: value::Codec<V>,
}

impl ValueCodec {
    fn new(codec: value::Codec<V>) -> Self {
        ValueCodec { codec }
    }
}

impl Decoder for ValueCodec {
    type Item = V;
    type Error = value::decoder::Error;
    fn decode(&mut self, bs: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut buf = &bs[..];
        let orig_len = buf.len();
        let res = self.codec.decode(&mut buf);
        let final_len = buf.len();
        bs.advance(orig_len - final_len);
        match res {
            Ok(v) => Ok(Some(v)),
            Err(value::codec::Error::Eof) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Encoder for ValueCodec {
    type Item = V;
    type Error = value::encoder::Error;
    fn encode(&mut self, item: Self::Item, bs: &mut BytesMut) -> Result<(), Self::Error> {
        bs.extend(self.codec.encode_bytes(&item)?);
        Ok(())
    }
}

struct Peer {
    id: ConnId,
    rx: Receiver<packets::Out>,
    dataspace: Sender<DataspaceMessage>,
    frames: Framed<TcpStream, ValueCodec>,
}

impl Peer {
    async fn new(id: ConnId, mut dataspace: Sender<DataspaceMessage>, stream: TcpStream) -> Self {
        let (tx, rx) = channel(1);
        let frames = Framed::new(stream, ValueCodec::new(value::Codec::new({
            let mut m = Map::new();
            m.insert(0, value::Value::symbol("Discard"));
            m.insert(1, value::Value::symbol("Capture"));
            m.insert(2, value::Value::symbol("Observe"));
            m
        })));
        dataspace.send(DataspaceMessage::Join(id, tx)).await.unwrap();
        Peer{ id, rx, dataspace, frames }
    }

    async fn run(&mut self) -> Result<(), std::io::Error> {
        println!("Got {:?} {:?}", self.id, &self.frames.get_ref());
        let mut running = true;
        while running {
            let mut to_send = Vec::new();
            select! {
                frame = self.frames.next().boxed().fuse() => match frame {
                    Some(res) => match res {
                        Ok(v) => {
                            println!("Input {}: {:?}", self.id, &v);
                            match value::from_value(&v) {
                                Ok(p) => {
                                    let m = DataspaceMessage::Input(self.id, p);
                                    self.dataspace.send(m).await.unwrap()
                                }
                                Err(e) => {
                                    to_send.push(packets::Out::Err(format!("{:?}", e)));
                                    running = false;
                                }
                            }
                        }
                        Err(value::codec::Error::Eof) => running = false,
                        Err(value::codec::Error::Io(e)) => return Err(e),
                        Err(value::codec::Error::Syntax(s)) => {
                            to_send.push(packets::Out::Err(s.to_string()));
                            running = false;
                        }
                    }
                    None => running = false,
                },
                msgopt = self.rx.recv().boxed().fuse() => {
                    match msgopt {
                        Some(msg) => to_send.push(msg),
                        None => /* weird. */ running = false,
                    }
                },
            }
            for v in to_send {
                if let packets::Out::Err(ref msg) = v {
                    println!("Connection {} crashed with error {:?}", self.id, msg);
                } else {
                    println!("Output {}: {:?}", self.id, &v);
                }
                self.frames.send(value::to_value(v).unwrap()).await?;
            }
        }
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let mut dataspace = self.dataspace.clone();
        let id = self.id;
        tokio::spawn(async move {
            let _ = dataspace.send(DataspaceMessage::Leave(id)).await;
        });
    }
}

struct Dataspace {
    rx: Receiver<DataspaceMessage>,
    peers: Map<ConnId, Sender<packets::Out>>,
}

impl Dataspace {
    fn new(rx: Receiver<DataspaceMessage>) -> Self {
        Dataspace { rx, peers: Map::new() }
    }

    async fn send(&mut self, i: ConnId, s: &mut Sender<packets::Out>, m: &packets::Out)
                  -> bool
    {
        match s.send(m.clone()).await {
            Ok(_) => true,
            Err(_) => { self.remove(i); false }
        }
    }

    async fn send_to(&mut self, i: ConnId, m: &packets::Out) -> bool {
        let mut ms = self.peers.get(&i).map(|s| s.clone());
        match ms {
            Some(ref mut s) => self.send(i, s, m).await,
            None => false,
        }
    }

    fn remove(&mut self, i: ConnId) {
        self.peers.remove(&i);
        // TODO: cleanup. Previously, this was:
        // self.pending.push(PeerMessage::Leave(i));
    }

    async fn run(&mut self) {
        loop {
            println!("Dataspace waiting for message ({} connected)", self.peers.len());
            let msg = self.rx.recv().await.unwrap();
            println!("Dataspace: {:?}", msg);
            match msg {
                DataspaceMessage::Join(i, s) => {
                    self.peers.insert(i, s);
                }
                DataspaceMessage::Input(i, p) => {
                    match p {
                        packets::In::Connect(dsname) => (),
                        packets::In::Turn(actions) => (),
                        packets::In::Ping() => { self.send_to(i, &packets::Out::Pong()).await; }
                        packets::In::Pong() => (),
                    }
                }
                DataspaceMessage::Leave(i) => self.remove(i),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let i = Index::new();

    // Unlike std channels, a zero buffer is not supported
    let (tx, rx) = channel(100); // but ugh a big buffer is needed to avoid deadlocks???
    tokio::spawn(async {
        Dataspace::new(rx).run().await;
    });

    let mut id = 0;

    let port = 8001;
    let mut listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("Listening on port {}", port);
    loop {
        let (stream, addr) = listener.accept().await?;
        let tx = tx.clone();
        let connid = id;
        id = id + 1;
        tokio::spawn(async move {
            match Peer::new(connid, tx, stream).await.run().await {
                Ok(_) => (),
                Err(e) => println!("Connection {:?} died with {:?}", addr, e),
            }
        });
    }
}
