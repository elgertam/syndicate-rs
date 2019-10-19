use super::{ConnId, V};
use super::packets;

use core::time::Duration;
use futures::select;
use preserves::value::{self, Map};
use std::sync::{Mutex, Arc};
use tokio::codec::Framed;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio::timer::Interval;

pub struct Peer {
    id: ConnId,
    tx: UnboundedSender<packets::Out>,
    rx: UnboundedReceiver<packets::Out>,
    frames: Framed<TcpStream, packets::Codec>,
}

fn err(s: &str) -> packets::Out {
    packets::Out::Err(s.into())
}

impl Peer {
    pub async fn new(id: ConnId, stream: TcpStream) -> Self {
        let (tx, rx) = unbounded_channel();
        let frames = Framed::new(stream, packets::Codec::new(value::Codec::new({
            let mut m = Map::new();
            m.insert(0, value::Value::symbol("Discard"));
            m.insert(1, value::Value::symbol("Capture"));
            m.insert(2, value::Value::symbol("Observe"));
            m
        })));
        Peer{ id, tx, rx, frames }
    }

    pub async fn run(&mut self, spaces: Arc<Mutex<Map<V, ()>>>) -> Result<(), std::io::Error> {
        println!("{:?}: {:?}", self.id, &self.frames.get_ref());

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
                        Err(packets::DecodeError::Read(value::decoder::Error::Eof)) => running = false,
                        Err(packets::DecodeError::Read(value::decoder::Error::Io(e))) => return Err(e),
                        Err(packets::DecodeError::Read(value::decoder::Error::Syntax(s))) => {
                            to_send.push(err(s));
                            running = false;
                        }
                        Err(packets::DecodeError::Parse(e, v)) => {
                            to_send.push(err(&format!(
                                "Packet deserialization error ({}) decoding {:?}", e, v)));
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
                    println!("{:?}: connection crashed: {}", self.id, msg);
                } else {
                    println!("{:?}: output {:?}", self.id, &v);
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
