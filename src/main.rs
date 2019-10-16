#![recursion_limit="512"]

mod bag;
mod skeleton;

use bytes::BytesMut;
use core::time::Duration;
use futures::select;
use preserves::value::{self, Map};
use std::io;
use std::sync::{Mutex, RwLock, Arc};
use tokio::codec::{Framed, Encoder, Decoder};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio::timer::Interval;

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
enum PacketDecodeError {
    Read(value::decoder::Error),
    Parse(value::error::Error),
}

impl From<io::Error> for PacketDecodeError {
    fn from(v: io::Error) -> Self {
        PacketDecodeError::Read(v.into())
    }
}

impl From<value::error::Error> for PacketDecodeError {
    fn from(v: value::error::Error) -> Self {
        PacketDecodeError::Parse(v)
    }
}

#[derive(Debug)]
enum PacketEncodeError {
    Write(value::encoder::Error),
    Unparse(value::error::Error),
}

impl From<io::Error> for PacketEncodeError {
    fn from(v: io::Error) -> Self {
        PacketEncodeError::Write(v.into())
    }
}

impl From<value::error::Error> for PacketEncodeError {
    fn from(v: value::error::Error) -> Self {
        PacketEncodeError::Unparse(v)
    }
}

impl From<PacketEncodeError> for io::Error {
    fn from(v: PacketEncodeError) -> Self {
        match v {
            PacketEncodeError::Write(e) => e,
            PacketEncodeError::Unparse(e) =>
                Self::new(io::ErrorKind::InvalidData, format!("{:?}", e)),
        }
    }
}

struct PacketCodec {
    codec: value::Codec<V>,
}

impl PacketCodec {
    fn new(codec: value::Codec<V>) -> Self {
        PacketCodec { codec }
    }
}

impl Decoder for PacketCodec {
    type Item = packets::In;
    type Error = PacketDecodeError;
    fn decode(&mut self, bs: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut buf = &bs[..];
        let orig_len = buf.len();
        let res = self.codec.decode(&mut buf);
        let final_len = buf.len();
        bs.advance(orig_len - final_len);
        match res {
            Ok(v) => Ok(Some(value::from_value(&v)?)),
            Err(value::decoder::Error::Eof) => Ok(None),
            Err(e) => Err(PacketDecodeError::Read(e)),
        }
    }
}

impl Encoder for PacketCodec {
    type Item = packets::Out;
    type Error = PacketEncodeError;
    fn encode(&mut self, item: Self::Item, bs: &mut BytesMut) -> Result<(), Self::Error> {
        let v: V = value::to_value(&item)?;
        bs.extend(self.codec.encode_bytes(&v)?);
        Ok(())
    }
}

fn err(s: &str) -> packets::Out {
    packets::Out::Err(s.into())
}

struct Peer {
    id: ConnId,
    tx: UnboundedSender<packets::Out>,
    rx: UnboundedReceiver<packets::Out>,
    frames: Framed<TcpStream, PacketCodec>,
}

impl Peer {
    async fn new(id: ConnId, stream: TcpStream) -> Self {
        let (tx, rx) = unbounded_channel();
        let frames = Framed::new(stream, PacketCodec::new(value::Codec::new({
            let mut m = Map::new();
            m.insert(0, value::Value::symbol("Discard"));
            m.insert(1, value::Value::symbol("Capture"));
            m.insert(2, value::Value::symbol("Observe"));
            m
        })));
        Peer{ id, tx, rx, frames }
    }

    async fn run(&mut self, spaces: Arc<Mutex<Map<V, ()>>>) -> Result<(), io::Error> {
        println!("{:?}: got {:?}", self.id, &self.frames.get_ref());

        let firstpacket = self.frames.next().await;
        let dsname = if let Some(Ok(packets::In::Connect(dsname))) = firstpacket {
            dsname
        } else {
            let e: String = format!("Expected initial Connect, got {:?}", firstpacket);
            println!("{:?}: {}", self.id, e);
            self.frames.send(err(&e)).await?;
            return Ok(())
        };

        let is_new = {
            let mut s = spaces.lock().unwrap();
            match s.get(&dsname) {
                Some(_) => false,
                None => {
                    s.insert(dsname.clone(), ());
                    true
                }
            }
        };

        println!("{:?}: connected to {} dataspace {:?}",
                 self.id,
                 if is_new { "new" } else { "existing" },
                 dsname);

        let mut ping_timer = Interval::new_interval(Duration::from_secs(60));

        let mut running = true;
        while running {
            let mut to_send = Vec::new();
            select! {
                _instant = ping_timer.next().boxed().fuse() => to_send.push(packets::Out::Ping()),
                frame = self.frames.next().boxed().fuse() => match frame {
                    Some(res) => match res {
                        Ok(p) => {
                            println!("{:?}: input {:?}", self.id, &p);
                            match p {
                                packets::In::Turn(actions) => (),
                                packets::In::Ping() => {
                                    to_send.push(packets::Out::Pong())
                                }
                                packets::In::Pong() => (),
                                packets::In::Connect(dsname) => {
                                    to_send.push(err("Unexpected Connect"));
                                    running = false;
                                }
                            }
                        }
                        Err(PacketDecodeError::Read(value::decoder::Error::Eof)) => running = false,
                        Err(PacketDecodeError::Read(value::decoder::Error::Io(e))) => return Err(e),
                        Err(PacketDecodeError::Read(value::decoder::Error::Syntax(s))) => {
                            to_send.push(err(s));
                            running = false;
                        }
                        Err(PacketDecodeError::Parse(e)) => {
                            to_send.push(err(&format!("Packet deserialization error: {:?}", e)));
                            running = false;
                        }
                    }
                    None => running = false,
                },
                msgopt = self.rx.recv().boxed().fuse() => {
                    match msgopt {
                        Some(msg) => to_send.push(msg),
                        None => {
                            /* weird. */
                            to_send.push(err("Outbound channel closed unexpectedly"));
                            running = false;
                        }
                    }
                },
            }
            for v in to_send {
                if let packets::Out::Err(ref msg) = v {
                    println!("{:?}: Connection crashed with error {:?}", self.id, msg);
                } else {
                    println!("{:?}: Output {:?}", self.id, &v);
                }
                self.frames.send(v).await?;
            }
        }
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spaces = Arc::new(Mutex::new(Map::new()));
    let mut id = 0;

    let port = 8001;
    let mut listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("Listening on port {}", port);
    loop {
        let (stream, addr) = listener.accept().await?;
        let connid = id;
        let spaces = Arc::clone(&spaces);
        id = id + 1;
        tokio::spawn(async move {
            match Peer::new(connid, stream).await.run(spaces).await {
                Ok(_) => (),
                Err(e) => println!("Connection {:?} died with {:?}", addr, e),
            }
        });
    }
}
