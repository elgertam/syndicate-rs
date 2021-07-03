use futures::FutureExt;
use futures::StreamExt;
use futures::select;
use futures::{Sink, SinkExt, Stream};

use preserves::value;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::actor::*;
use super::config;
use super::error::Error;
use super::error::error;
use super::packets;

pub struct Peer<I, O>
where I: Stream<Item = Result<packets::Packet, Error>> + Send,
      O: Sink<packets::Packet, Error = Error>,
{
    i: Pin<Box<I>>,
    o: Pin<Box<O>>,
    ds: Arc<Ref>,
    config: Arc<config::ServerConfig>,
}

impl<I, O> Peer<I, O>
where I: Stream<Item = Result<packets::Packet, Error>> + Send,
      O: Sink<packets::Packet, Error = Error>,
{
    pub fn new(i: I, o: O, ds: Arc<Ref>, config: Arc<config::ServerConfig>) -> Self {
        Peer{ i: Box::pin(i), o: Box::pin(o), ds, config }
    }

    pub async fn run(mut self) -> Result<(), packets::Error> {
        let queue_depth = Arc::new(AtomicUsize::new(0));

        let mut running = true;
        let mut overloaded = None;
        let mut previous_sample = None;
        while running {
            let mut to_send = Vec::new();

            let queue_depth_sample = queue_depth.load(Ordering::Relaxed);
            if queue_depth_sample > self.config.overload_threshold {
                let n = overloaded.unwrap_or(0);
                tracing::warn!(turns=n, queue_depth=queue_depth_sample, "overloaded");
                if n == self.config.overload_turn_limit {
                    to_send.push(error("Overloaded", queue_depth_sample as u128));
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
                frame = self.i.next().fuse() => match frame {
                    Some(res) => match res {
                        Ok(p) => {
                            tracing::trace!(packet = debug(&p), "input");
                            match p {
                                packets::Packet::Turn(b) => {
                                    let packets::Turn(actions) = &*b;
                                    /* ... */
                                }
                                packets::Packet::Error(b) => {
                                    let e = &*b;
                                    /* ... */
                                }
                            }
                        }
                        Err(e) => return Err(e),
                    }
                    None => {
                        tracing::trace!("remote has closed");
                        running = false;
                    }
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
                        to_send.push(error("Outbound channel closed unexpectedly", value::FALSE.clone()));
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
