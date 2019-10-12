#![recursion_limit="256"]

mod bag;
mod skeleton;

use bytes::BytesMut;
use preserves::value::{self, NestedValue};
use std::collections::BTreeMap;
use std::sync::Arc;
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
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Error(pub String);
}

#[derive(Debug)]
pub enum RelayMessage {
    Hello(ConnId, Sender<Arc<PeerMessage>>),
    Speak(ConnId, V),
    Goodbye(ConnId),
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum PeerMessage {
    Join(ConnId),
    Speak(ConnId, V),
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
    rx: Receiver<Arc<PeerMessage>>,
    relay: Sender<RelayMessage>,
    frames: Framed<TcpStream, ValueCodec>,
}

impl Peer {
    async fn new(id: ConnId, mut relay: Sender<RelayMessage>, stream: TcpStream) -> Self {
        let (tx, rx) = channel(1);
        let frames = Framed::new(stream, ValueCodec::new(value::Codec::without_placeholders()));
        relay.send(RelayMessage::Hello(id, tx)).await.unwrap();
        Peer{ id, rx, relay, frames }
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
                            if (v.value().as_symbol() == Some(&"die".to_string())) {
                                panic!();
                            } else {
                                self.relay.send(RelayMessage::Speak(self.id, v)).await.unwrap()
                            }
                        }
                        Err(value::codec::Error::Eof) => running = false,
                        Err(value::codec::Error::Io(e)) => return Err(e),
                        Err(value::codec::Error::Syntax(s)) => {
                            let v = value::to_value(packets::Error(s.to_string())).unwrap();
                            to_send.push(v);
                            running = false;
                        }
                    }
                    None => running = false,
                },
                msgopt = self.rx.recv().boxed().fuse() => {
                    println!("MSGOPT {:?}", &msgopt);
                    match msgopt {
                        Some(msg) => to_send.push(value::to_value(&*msg).unwrap()),
                        None => /* weird. */ running = false,
                    }
                },
            }
            for v in to_send { self.frames.send(v).await?; }
        }
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let mut relay = self.relay.clone();
        let id = self.id;
        tokio::spawn(async move {
            let _ = relay.send(RelayMessage::Goodbye(id)).await;
        });
    }
}

struct Relay {
    rx: Receiver<RelayMessage>,
    peers: BTreeMap<ConnId, Sender<Arc<PeerMessage>>>,
    pending: Vec<PeerMessage>,
}

impl Relay {
    fn new(rx: Receiver<RelayMessage>) -> Self {
        Relay { rx, peers: BTreeMap::new(), pending: Vec::new() }
    }

    async fn send(&mut self, i: ConnId, s: &mut Sender<Arc<PeerMessage>>, m: &Arc<PeerMessage>)
                  -> bool
    {
        match s.send(Arc::clone(m)).await {
            Ok(_) => true,
            Err(_) => { self.remove(i); false }
        }
    }

    fn remove(&mut self, i: ConnId) {
        self.peers.remove(&i);
        self.pending.push(PeerMessage::Leave(i));
    }

    async fn broadcast(&mut self, m: &Arc<PeerMessage>) {
        for (i, ref mut s) in self.peers.clone() {
            self.send(i, s, m).await;
        }
    }

    async fn run(&mut self) {
        loop {
            println!("Relay waiting for message ({} connected)", self.peers.len());
            let msg = self.rx.recv().await.unwrap();
            println!("Relay: {:?}", msg);
            match msg {
                RelayMessage::Hello(i, mut s) => {
                    let mut ok = true;
                    let i_join = &Arc::new(PeerMessage::Join(i));
                    for (p, ref mut r) in self.peers.clone() {
                        ok = ok && self.send(i, &mut s, &Arc::new(PeerMessage::Join(p))).await;
                        self.send(p, r, i_join).await;
                    }
                    ok = ok && self.send(i, &mut s, i_join).await;
                    if ok {
                        self.peers.insert(i, s);
                    }
                }
                RelayMessage::Speak(i, v) => {
                    self.broadcast(&Arc::new(PeerMessage::Speak(i, v))).await;
                }
                RelayMessage::Goodbye(i) => self.remove(i),
            }
            while let Some(m) = self.pending.pop() {
                self.broadcast(&Arc::new(m)).await;
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
        Relay::new(rx).run().await;
    });

    let mut id = 0;

    let mut listener = TcpListener::bind("0.0.0.0:5889").await?;
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
