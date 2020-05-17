use super::V;
use super::ConnId;
use super::dataspace;
use super::packets;
use super::spaces;

use core::time::Duration;
use futures::{Sink, SinkExt, Stream};
use futures::FutureExt;
use futures::select;
use preserves::value;
use std::pin::Pin;
use std::sync::{Mutex, Arc};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio::time::interval;

pub type ResultC2S = Result<packets::C2S, packets::DecodeError>;

pub struct Peer<I, O>
where I: Stream<Item = ResultC2S> + Send,
      O: Sink<packets::S2C, Error = packets::EncodeError>,
{
    id: ConnId,
    tx: UnboundedSender<packets::S2C>,
    rx: UnboundedReceiver<packets::S2C>,
    i: Pin<Box<I>>,
    o: Pin<Box<O>>,
    space: Option<dataspace::DataspaceRef>,
}

fn err(s: &str, ctx: V) -> packets::S2C {
    packets::S2C::Err(s.into(), ctx)
}

impl<I, O> Peer<I, O>
where I: Stream<Item = ResultC2S> + Send,
      O: Sink<packets::S2C, Error = packets::EncodeError>,
{
    pub fn new(id: ConnId, i: I, o: O) -> Self {
        let (tx, rx) = unbounded_channel();
        Peer{ id, tx, rx, i: Box::pin(i), o: Box::pin(o), space: None }
    }

    pub async fn run(&mut self, spaces: Arc<Mutex<spaces::Spaces>>) -> Result<(), std::io::Error> {
        let firstpacket = self.i.next().await;
        let dsname = if let Some(Ok(packets::C2S::Connect(dsname))) = firstpacket {
            dsname
        } else {
            let e = format!("Expected initial Connect, got {:?}", firstpacket);
            self.o.send(err(&e, value::Value::from(false).wrap())).await?;
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        };

        self.space = Some(spaces.lock().unwrap().lookup(&dsname));
        self.space.as_ref().unwrap().write().unwrap().register(self.id, self.tx.clone());

        let mut ping_timer = interval(Duration::from_secs(60));

        let mut running = true;
        while running {
            let mut to_send = Vec::new();
            select! {
                _instant = ping_timer.next().boxed().fuse() => to_send.push(packets::S2C::Ping()),
                frame = self.i.next().fuse() => match frame {
                    Some(res) => match res {
                        Ok(p) => {
                            // println!("{:?}: input {:?}", self.id, &p);
                            match p {
                                packets::C2S::Turn(actions) => {
                                    match self.space.as_ref().unwrap().write().unwrap()
                                        .turn(self.id, actions)
                                    {
                                        Ok(()) => (),
                                        Err((msg, ctx)) => {
                                            to_send.push(err(&msg, ctx));
                                            running = false;
                                        }
                                    }
                                }
                                packets::C2S::Ping() =>
                                    to_send.push(packets::S2C::Pong()),
                                packets::C2S::Pong() =>
                                    (),
                                packets::C2S::Connect(_) => {
                                    to_send.push(err("Unexpected Connect", value::to_value(p).unwrap()));
                                    running = false;
                                }
                            }
                        }
                        Err(packets::DecodeError::Read(value::decoder::Error::Eof)) => running = false,
                        Err(packets::DecodeError::Read(value::decoder::Error::Io(e))) => return Err(e),
                        Err(packets::DecodeError::Read(value::decoder::Error::Syntax(s))) => {
                            to_send.push(err(s, value::Value::from(false).wrap()));
                            running = false;
                        }
                        Err(packets::DecodeError::Parse(e, v)) => {
                            to_send.push(err(&format!("Packet deserialization error: {}", e), v));
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
                            to_send.push(err("Outbound channel closed unexpectedly",
                                             value::Value::from(false).wrap()));
                            running = false;
                        }
                    }
                },
            }
            for v in to_send {
                if let packets::S2C::Err(ref msg, ref ctx) = v {
                    println!("{:?}: connection crashed: {}; context {:?}", self.id, msg, ctx);
                } else {
                    // println!("{:?}: output {:?}", self.id, &v);
                    ()
                }
                self.o.send(v).await?;
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

impl<I, O> Drop for Peer<I, O>
where I: Stream<Item = ResultC2S> + Send,
      O: Sink<packets::S2C, Error = packets::EncodeError>,
{
    fn drop(&mut self) {
        if let Some(ref s) = self.space {
            s.write().unwrap().deregister(self.id);
        }
    }
}
