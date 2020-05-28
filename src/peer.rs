use super::V;
use super::ConnId;
use super::dataspace;
use super::packets;
use super::spaces;
use super::config;

use core::time::Duration;
use futures::{Sink, SinkExt, Stream};
use futures::FutureExt;
use futures::select;
use preserves::value;
use std::pin::Pin;
use std::sync::{Mutex, Arc, atomic::{AtomicUsize, Ordering}};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver, error::TryRecvError};
use tokio::time::interval;

pub type ResultC2S = Result<packets::C2S, packets::Error>;

pub struct Peer<I, O>
where I: Stream<Item = ResultC2S> + Send,
      O: Sink<packets::S2C, Error = packets::Error>,
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
      O: Sink<packets::S2C, Error = packets::Error>,
{
    pub fn new(id: ConnId, i: I, o: O) -> Self {
        let (tx, rx) = unbounded_channel();
        Peer{ id, tx, rx, i: Box::pin(i), o: Box::pin(o), space: None }
    }

    pub async fn run(&mut self, spaces: Arc<Mutex<spaces::Spaces>>, config: &config::ServerConfig) ->
        Result<(), packets::Error>
    {
        let firstpacket = self.i.next().await;
        let dsname = if let Some(Ok(packets::C2S::Connect(dsname))) = firstpacket {
            dsname
        } else {
            let e = format!("Expected initial Connect, got {:?}", firstpacket);
            self.o.send(err(&e, value::FALSE.clone())).await?;
            return Err(preserves::error::syntax_error(&e))
        };

        self.space = Some(spaces.lock().unwrap().lookup(&dsname));
        let queue_depth = Arc::new(AtomicUsize::new(0));
        self.space.as_ref().unwrap().write().unwrap().register(
            self.id,
            self.tx.clone(),
            Arc::clone(&queue_depth));

        let mut ping_timer = interval(Duration::from_secs(60));

        let mut running = true;
        let mut overloaded = None;
        let mut previous_sample = None;
        while running {
            let mut to_send = Vec::new();

            let queue_depth_sample = queue_depth.load(Ordering::Relaxed);
            if queue_depth_sample > config.overload_threshold {
                let n = overloaded.unwrap_or(0);
                tracing::warn!(turns=n, queue_depth=queue_depth_sample, "overloaded");
                if n == config.overload_turn_limit {
                    to_send.push(err("Overloaded",
                                     value::Value::from(queue_depth_sample as u64).wrap()));
                    running = false;
                } else {
                    if queue_depth_sample > previous_sample.unwrap_or(0) {
                        overloaded = Some(n + 1)
                    } else {
                        overloaded = Some(0)
                    }
                }
            } else {
                if let Some(_) = overloaded {
                    tracing::info!(queue_depth=queue_depth_sample, "recovered");
                }
                overloaded = None;
            }
            previous_sample = Some(queue_depth_sample);

            select! {
                _instant = ping_timer.next().boxed().fuse() => to_send.push(packets::S2C::Ping()),
                frame = self.i.next().fuse() => match frame {
                    Some(res) => match res {
                        Ok(p) => {
                            tracing::trace!(packet = debug(&p), "input");
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
                                    to_send.push(err("Unexpected Connect", value::to_value(p)));
                                    running = false;
                                }
                            }
                        }
                        Err(e) if preserves::error::is_eof_error(&e) => {
                            tracing::trace!("eof");
                            running = false;
                        }
                        Err(e) if preserves::error::is_syntax_error(&e) => {
                            to_send.push(err(&e.to_string(), value::FALSE.clone()));
                            running = false;
                        }
                        Err(e) => {
                            if preserves::error::is_io_error(&e) {
                                return Err(e);
                            } else {
                                to_send.push(err(&format!("Packet deserialization error: {}", e),
                                                 value::FALSE.clone()));
                                running = false;
                            }
                        }
                    }
                    None => running = false,
                },
                msgopt = self.rx.recv().boxed().fuse() => {
                    let mut ok = true;
                    match msgopt {
                        Some(msg) => {
                            to_send.push(msg);
                            loop {
                                match self.rx.try_recv() {
                                    Ok(m) => to_send.push(m),
                                    Err(TryRecvError::Empty) => {
                                        queue_depth.store(0, Ordering::Relaxed);
                                        break;
                                    }
                                    Err(TryRecvError::Closed) => {
                                        ok = false;
                                        break;
                                    }
                                }
                            }
                        }
                        None => ok = false,
                    }
                    if !ok {
                        /* weird. */
                        to_send.push(err("Outbound channel closed unexpectedly", value::FALSE.clone()));
                        running = false;
                    }
                },
            }
            for v in to_send {
                if let packets::S2C::Err(ref msg, ref ctx) = v {
                    tracing::error!(context = debug(ctx), msg = display(msg), "error");
                } else {
                    tracing::trace!(packet = debug(&v), "output");
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
      O: Sink<packets::S2C, Error = packets::Error>,
{
    fn drop(&mut self) {
        if let Some(ref s) = self.space {
            s.write().unwrap().deregister(self.id);
        }
    }
}
