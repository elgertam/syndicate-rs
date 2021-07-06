use bytes::Buf;
use bytes::BytesMut;

use crate::actor::*;
use crate::error::Error;
use crate::schemas::internal_protocol::*;
use crate::schemas::tunnel_relay;

use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;

use preserves::value::BinarySource;
use preserves::value::BytesBinarySource;
use preserves::value::DomainDecode;
use preserves::value::DomainEncode;
use preserves::value::IOValue;
use preserves::value::Map;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::PackedReader;
use preserves::value::PackedWriter;
use preserves::value::Reader;
use preserves::value::Writer;
use preserves_schema::support::lazy_static;

use std::convert::TryFrom;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use tracing;

struct WireSymbol {
    oid: Oid,
    obj: Arc<Ref>,
    ref_count: atomic::AtomicUsize,
}

struct Membrane {
    oid_map: Map<Oid, Arc<WireSymbol>>,
    ref_map: Map<Arc<Ref>, Arc<WireSymbol>>,
}

struct Membranes {
    exported: Membrane,
    imported: Membrane,
}

pub enum Input {
    Packets(Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>),
    Bytes(Pin<Box<dyn AsyncRead + Send>>),
}

pub enum Output {
    Packets(Pin<Box<dyn Sink<Vec<u8>, Error = Error> + Send>>),
    Bytes(Pin<Box<dyn AsyncWrite + Send>>),
}

// There are other kinds of relay. This one has exactly two participants connected to each other.
pub struct TunnelRelay
{
    input_buffer: BytesMut,
    o: Output,
    inbound_assertions: Map</* remote */ Handle, (/* local */ Handle, Vec<Arc<WireSymbol>>)>,
    outbound_assertions: Map<Handle, Vec<Arc<WireSymbol>>>,
    membranes: Membranes,
    pending_outbound: Vec<TurnEvent>,
}

lazy_static! {
    static ref INERT_REF: Arc<Ref> = {
        struct InertEntity;
        impl crate::actor::Entity for InertEntity {}
        let mut ac = crate::actor::Actor::new();
        ac.create(InertEntity)
    };
}

//---------------------------------------------------------------------------

impl WireSymbol {
    fn acquire(&self) {
        self.ref_count.fetch_add(1, atomic::Ordering::Relaxed);
    }

    fn release(&self) -> bool {
        self.ref_count.fetch_sub(1, atomic::Ordering::Relaxed) == 1
    }
}

impl Membrane {
    fn new() -> Self {
        Membrane {
            oid_map: Map::new(),
            ref_map: Map::new(),
        }
    }

    fn insert(&mut self, oid: Oid, obj: Arc<Ref>) -> Arc<WireSymbol> {
        let ws = Arc::new(WireSymbol {
            oid: oid.clone(),
            obj: Arc::clone(&obj),
            ref_count: atomic::AtomicUsize::new(0),
        });
        self.oid_map.insert(oid, Arc::clone(&ws));
        self.ref_map.insert(obj, Arc::clone(&ws));
        ws
    }

    fn release(&mut self, ws: &Arc<WireSymbol>) {
        if ws.release() {
            self.oid_map.remove(&ws.oid);
            self.ref_map.remove(&ws.obj);
        }
    }
}

impl TunnelRelay {
    pub fn run(ac: &mut Actor, i: Input, o: Output) -> ActorResult {
        let tr = ac.create(TunnelRelay {
            input_buffer: BytesMut::with_capacity(1024),
            o,
            inbound_assertions: Map::new(),
            outbound_assertions: Map::new(),
            membranes: Membranes {
                exported: Membrane::new(),
                imported: Membrane::new(),
            },
            pending_outbound: Vec::new(),
        });
        ac.add_exit_hook(&tr.target);
        ac.linked_task(tracing::info_span!("reader"), input_loop(i, tr));
        Ok(())
    }

    fn handle_packet(&mut self, p: Packet) -> ActorResult {
        match p {
            Packet::Error(b) => {
                tracing::info!(message = debug(b.message.clone()),
                               detail = debug(b.detail.clone()),
                               "received Error from peer");
                Err(*b)
            },
            Packet::Turn(b) => {
                let Turn(events) = *b;
                for TurnEvent { oid, event } in events {
                    tracing::info!(oid = debug(oid), event = debug(event))
                }
                Ok(())
            }
        }
    }

    pub fn send_event(&mut self, oid: Oid, event: Event) -> bool {
        let need_flush = self.pending_outbound.is_empty();
        self.pending_outbound.push(TurnEvent { oid, event });
        need_flush
    }

    pub fn decode_packet(&mut self, bs: &[u8]) -> Result<Packet, Error> {
        let mut src = BytesBinarySource::new(bs);
        Ok(Packet::try_from(&src.packed::<_, _Any, _>(&mut self.membranes).demand_next(false)?)?)
    }

    fn encode_packet(&mut self, p: Packet) -> Result<Vec<u8>, Error> {
        Ok(PackedWriter::encode::<_, _Any, _>(&mut self.membranes, &_Any::from(&p))?)
    }

    pub async fn send_packet(&mut self, p: Packet) -> ActorResult {
        let bs = self.encode_packet(p)?;
        match &mut self.o {
            Output::Packets(sink) => Ok(sink.send(bs).await?),
            Output::Bytes(w) => Ok(w.write_all(&bs).await?),
        }
    }
}

impl DomainDecode<_Ptr> for Membranes {
    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        src: &'src mut S,
        _read_annotations: bool,
    ) -> io::Result<_Ptr> {
        let v: IOValue = PackedReader::new(src, NoEmbeddedDomainCodec).demand_next(false)?;
        Ok(Arc::new(_Dom::try_from(&v)?))
    }
}

impl DomainEncode<_Ptr> for Membranes {
    fn encode_embedded<W: Writer>(
        &mut self,
        w: &mut W,
        d: &_Ptr,
    ) -> io::Result<()> {
        w.write(&mut NoEmbeddedDomainCodec, &IOValue::from(d.as_ref()))
    }
}

pub async fn input_loop(
    i: Input,
    relay: Arc<Ref>,
) -> ActorResult {
    fn s<M: Into<_Any>>(relay: &Arc<Ref>, m: M) {
        relay.external_event(Event::Message(Box::new(Message { body: Assertion(m.into()) })))
    }

    match i {
        Input::Packets(mut src) => {
            loop {
                match src.next().await {
                    None => {
                        s(&relay, &tunnel_relay::Input::Eof);
                        return Ok(());
                    }
                    Some(bs) => s(&relay, &tunnel_relay::Input::Packet { bs: bs? }),
                }
            }
        }
        Input::Bytes(mut r) => {
            let mut buf = BytesMut::with_capacity(1024);
            loop {
                buf.reserve(1024);
                let n = r.read_buf(&mut buf).await?;
                match n {
                    0 => {
                        s(&relay, &tunnel_relay::Input::Eof);
                        return Ok(());
                    }
                    _ => {
                        while buf.has_remaining() {
                            let bs = buf.chunk();
                            let n = bs.len();
                            s(&relay, &tunnel_relay::Input::Segment { bs: bs.to_vec() });
                            buf.advance(n);
                        }
                    }
                }
            }
        }
    }
}

impl Entity for TunnelRelay {
    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        if let Ok(m) = tunnel_relay::Input::try_from(&m) {
            match m {
                tunnel_relay::Input::Eof => {
                    tracing::info!("eof");
                    t.actor.shutdown();
                }
                tunnel_relay::Input::Packet { bs } => {
                    let p = self.decode_packet(&bs)?;
                    self.handle_packet(p)?
                }
                tunnel_relay::Input::Segment { bs } => {
                    self.input_buffer.extend_from_slice(&bs);
                    loop {
                        let (e, count) = {
                            let mut src = BytesBinarySource::new(&self.input_buffer);
                            let mut r = src.packed::<_, _Any, _>(&mut self.membranes);
                            let e = r.next(false)?;
                            (e, r.source.index)
                        };
                        match e {
                            None => break,
                            Some(item) => {
                                self.input_buffer.advance(count);
                                self.handle_packet(Packet::try_from(&item)?)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn exit_hook(&mut self, _t: &mut Activation, exit_status: &ActorResult) -> BoxFuture<ActorResult> {
        if let Err(e) = exit_status {
            let e = e.clone();
            Box::pin(self.send_packet(Packet::Error(Box::new(e))))
        } else {
            Box::pin(ready(Ok(())))
        }
    }
}
