use bytes::Buf;
use bytes::BytesMut;

use crate::actor::*;
use crate::error::Error;
use crate::error::error;
use crate::schemas::internal_protocol::*;
use crate::schemas::sturdy;
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
use preserves::value::NestedValue;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::PackedReader;
use preserves::value::PackedWriter;
use preserves::value::Reader;
use preserves::value::Writer;
use preserves::value::signed_integer::SignedInteger;

use std::convert::TryFrom;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};

use tracing;

struct WireSymbol {
    oid: sturdy::Oid,
    obj: Arc<Ref>,
    ref_count: AtomicUsize,
}

struct Membrane {
    oid_map: Map<sturdy::Oid, Arc<WireSymbol>>,
    ref_map: Map<Arc<Ref>, Arc<WireSymbol>>,
}

struct Membranes {
    exported: Membrane,
    imported: Membrane,
    next_export_oid: usize,
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
    self_ref: Arc<Ref>,
    input_buffer: BytesMut,
    inbound_assertions: Map</* remote */ Handle, (/* local */ Handle, Vec<Arc<WireSymbol>>)>,
    outbound_assertions: Map<Handle, Vec<Arc<WireSymbol>>>,
    membranes: Membranes,
    pending_outbound: Vec<TurnEvent>,
    output: UnboundedSender<Vec<u8>>,
}

struct RelayEntity {
    relay_ref: Arc<Ref>,
    oid: sturdy::Oid,
}

//---------------------------------------------------------------------------

impl WireSymbol {
    fn acquire(&self) {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
    }

    fn release(&self) -> bool {
        self.ref_count.fetch_sub(1, Ordering::SeqCst) == 1
    }
}

impl Membrane {
    fn new() -> Self {
        Membrane {
            oid_map: Map::new(),
            ref_map: Map::new(),
        }
    }

    fn insert(&mut self, oid: sturdy::Oid, obj: Arc<Ref>) -> Arc<WireSymbol> {
        let ws = Arc::new(WireSymbol {
            oid: oid.clone(),
            obj: Arc::clone(&obj),
            ref_count: AtomicUsize::new(0),
        });
        self.oid_map.insert(oid, Arc::clone(&ws));
        self.ref_map.insert(obj, Arc::clone(&ws));
        ws
    }

    fn acquire(&mut self, r: &Arc<Ref>) -> Arc<WireSymbol> {
        let ws = self.ref_map.get(r).expect("WireSymbol must be present at acquire() time");
        ws.acquire();
        Arc::clone(ws)
    }

    fn release(&mut self, ws: &Arc<WireSymbol>) {
        if ws.release() {
            self.oid_map.remove(&ws.oid);
            self.ref_map.remove(&ws.obj);
        }
    }
}

impl TunnelRelay {
    pub fn run(
        t: &mut Activation,
        i: Input,
        o: Output,
        initial_ref: Option<Arc<Ref>>,
        initial_oid: Option<sturdy::Oid>,
    ) -> Option<Arc<Ref>> {
        let (output_tx, output_rx) = unbounded_channel();
        let mut tr = TunnelRelay {
            self_ref: Arc::clone(&*INERT_REF), /* placeholder */
            input_buffer: BytesMut::with_capacity(1024),
            output: output_tx,
            inbound_assertions: Map::new(),
            outbound_assertions: Map::new(),
            membranes: Membranes {
                exported: Membrane::new(),
                imported: Membrane::new(),
                next_export_oid: 0,
            },
            pending_outbound: Vec::new(),
        };
        if let Some(ir) = initial_ref {
            tr.membranes.export_ref(ir, true);
        }
        let mut result = None;
        let tr_ref = t.actor.create_rec(tr, |ac, tr, tr_ref| {
            tr.self_ref = Arc::clone(tr_ref);
            if let Some(io) = initial_oid {
                result = Some(Arc::clone(&tr.membranes.import_oid(ac, tr_ref, io).obj));
            }
        });
        t.actor.add_exit_hook(&tr_ref.target);
        t.actor.linked_task(crate::name!("writer"), output_loop(o, output_rx));
        t.actor.linked_task(crate::name!("reader"), input_loop(i, tr_ref));
        result
    }

    fn handle_inbound_packet(&mut self, t: &mut Activation, p: Packet) -> ActorResult {
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
                    let target = match self.membranes.exported.oid_map.get(&sturdy::Oid(oid.0.clone())) {
                        Some(ws) => &ws.obj,
                        None => return Err(error("Cannot deliver event: nonexistent oid",
                                                 _Any::from(&TurnEvent { oid, event }))),
                    };
                    match event {
                        Event::Assert(b) => {
                            let Assert { assertion: Assertion(a), handle: remote_handle } = *b;
                            let mut imported = vec![];
                            let imported_membrane = &mut self.membranes.imported;
                            a.foreach_embedded::<_, Error>(&mut |r| {
                                Ok(imported.push(imported_membrane.acquire(r)))
                            })?;
                            let local_handle = t.assert(target, a);
                            if let Some(_) = self.inbound_assertions.insert(remote_handle, (local_handle, imported)) {
                                return Err(error("Assertion with duplicate handle", _Any::new(false)));
                            }
                        }
                        Event::Retract(b) => {
                            let Retract { handle: remote_handle } = *b;
                            let (local_handle, imported) = match self.inbound_assertions.remove(&remote_handle) {
                                None => return Err(error("Retraction of nonexistent handle", _Any::from(&remote_handle))),
                                Some(wss) => wss,
                            };
                            for ws in imported.into_iter() {
                                self.membranes.imported.release(&ws);
                            }
                            t.retract(local_handle);
                        }
                        Event::Message(b) => {
                            let Message { body: Assertion(a) } = *b;
                            let imported_membrane = &mut self.membranes.imported;
                            a.foreach_embedded(&mut |r| {
                                let ws = imported_membrane.acquire(r);
                                match ws.ref_count.load(Ordering::SeqCst) {
                                    1 => Err(error("Cannot receive transient reference", _Any::new(false))),
                                    _ => Ok(())
                                }
                            })?;
                            t.message(target, a);
                        }
                        Event::Sync(b) => {
                            let Sync { peer } = *b;
                            self.membranes.imported.acquire(&peer);
                            struct SyncPeer {
                                tr: Arc<Ref>,
                                peer: Arc<Ref>,
                            }
                            impl Entity for SyncPeer {
                                fn message(&mut self, t: &mut Activation, a: _Any) -> ActorResult {
                                    if let Some(true) = a.value().as_boolean() {
                                        t.message(&self.peer, _Any::new(true));
                                        t.message(&self.tr, &tunnel_relay::SyncGc {
                                            peer: Arc::clone(&self.peer)
                                        });
                                    }
                                    Ok(())
                                }
                            }
                            let k = t.actor.create(SyncPeer {
                                tr: Arc::clone(&self.self_ref),
                                peer: Arc::clone(&peer),
                            });
                            t.sync(&peer, k);
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn handle_outbound_event(&mut self, t: &mut Activation, mut event: Event) -> Result<Event, Error> {
        match &mut event {
            Event::Assert(b) => {
                let Assert { assertion: Assertion(a), handle } = &**b;
                let mut outbound = Vec::new();
                a.foreach_embedded::<_, Error>(
                    &mut |r| Ok(outbound.push(self.membranes.export_ref(Arc::clone(r), true))))?;
                self.outbound_assertions.insert(handle.clone(), outbound);
            }
            Event::Retract(b) => {
                let Retract { handle } = &**b;
                if let Some(outbound) = self.outbound_assertions.remove(handle) {
                    for ws in outbound.into_iter() {
                        self.membranes.exported.release(&ws);
                    }
                }
            }
            Event::Message(b) => {
                let Message { body: Assertion(a) } = &**b;
                a.foreach_embedded(&mut |r| {
                    let ws = self.membranes.export_ref(Arc::clone(r), false);
                    match ws.ref_count.load(Ordering::SeqCst) {
                        0 => Err(error("Cannot send transient reference", _Any::new(false))),
                        _ => Ok(())
                    }
                })?;
            },
            Event::Sync(_b) => panic!("TODO not yet implemented"),
        }
        Ok(event)
    }

    fn encode_packet(&mut self, p: Packet) -> Result<Vec<u8>, Error> {
        let item = _Any::from(&p);
        tracing::trace!(packet = debug(&item), "<--");
        Ok(PackedWriter::encode::<_, _Any, _>(&mut self.membranes, &item)?)
    }

    pub fn send_packet(&mut self, p: Packet) -> ActorResult {
        let bs = self.encode_packet(p)?;
        let _ = self.output.send(bs);
        Ok(())
    }
}

impl Membranes {
    fn export_ref(&mut self, obj: Arc<Ref>, and_acquire: bool) -> Arc<WireSymbol> {
        let ws = match self.exported.ref_map.get(&obj) {
            None => {
                let oid = sturdy::Oid(SignedInteger::from(self.next_export_oid as u128));
                self.next_export_oid += 1;
                self.exported.insert(oid, obj)
            }
            Some(ws) => Arc::clone(ws)
        };
        if and_acquire {
            ws.acquire();
        }
        ws
    }

    fn import_oid(
        &mut self,
        ac: &mut Actor,
        relay_ref: &Arc<Ref>,
        oid: sturdy::Oid,
    ) -> Arc<WireSymbol> {
        let obj = ac.create(RelayEntity { relay_ref: Arc::clone(relay_ref), oid: oid.clone() });
        self.imported.insert(oid, obj)
    }

    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        t: &mut Activation,
        relay_ref: &Arc<Ref>,
        src: &'src mut S,
        _read_annotations: bool,
    ) -> io::Result<_Ptr> {
        let v: IOValue = PackedReader::new(src, NoEmbeddedDomainCodec).demand_next(false)?;
        match sturdy::WireRef::try_from(&v)? {
            sturdy::WireRef::Mine{ oid: b } => {
                let oid = *b;
                match self.imported.oid_map.get(&oid) {
                    Some(ws) => Ok(Arc::clone(&ws.obj)),
                    None => Ok(Arc::clone(&self.import_oid(t.actor, relay_ref, oid).obj)),
                }
            }
            sturdy::WireRef::Yours { oid: b, attenuation } => {
                let oid = *b;
                match self.exported.oid_map.get(&oid) {
                    Some(ws) => {
                        if attenuation.is_empty() {
                            Ok(Arc::clone(&ws.obj))
                        } else {
                            // TODO
                            panic!("Non-empty attenuation not yet implemented")
                        }
                    }
                    None => Ok(Arc::clone(&*INERT_REF)),
                }
            }
        }
    }
}

struct ActivatedMembranes<'a, 'activation, 'm>(&'a mut Activation<'activation>,
                                               &'m Arc<Ref>,
                                               &'m mut Membranes);

impl<'a, 'activation, 'm> DomainDecode<_Ptr> for ActivatedMembranes<'a, 'activation, 'm> {
    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        src: &'src mut S,
        read_annotations: bool,
    ) -> io::Result<_Ptr> {
        self.2.decode_embedded(self.0, self.1, src, read_annotations)
    }
}

impl DomainEncode<_Ptr> for Membranes {
    fn encode_embedded<W: Writer>(
        &mut self,
        w: &mut W,
        d: &_Ptr,
    ) -> io::Result<()> {
        w.write(&mut NoEmbeddedDomainCodec, &_Any::from(&match self.exported.ref_map.get(d) {
            Some(ws) => sturdy::WireRef::Mine { oid: Box::new(ws.oid.clone()) },
            None => match self.imported.ref_map.get(d) {
                Some(ws) => {
                    // TODO: attenuation check
                    sturdy::WireRef::Yours { oid: Box::new(ws.oid.clone()), attenuation: vec![] }
                }
                None =>
                    sturdy::WireRef::Mine { oid: Box::new(self.export_ref(Arc::clone(d), false).oid.clone()) },
            }
        }))
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

pub async fn output_loop(
    mut o: Output,
    mut output_rx: UnboundedReceiver<Vec<u8>>,
) -> ActorResult {
    loop {
        match output_rx.recv().await {
            None =>
                return Ok(()),
            Some(bs) => match &mut o {
                Output::Packets(sink) => sink.send(bs).await?,
                Output::Bytes(w) => w.write_all(&bs).await?,
            }
        }
    }
}

impl Entity for TunnelRelay {
    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        if let Ok(m) = tunnel_relay::RelayProtocol::try_from(&m) {
            match m {
                tunnel_relay::RelayProtocol::Input(b) => match *b {
                    tunnel_relay::Input::Eof => {
                        t.actor.shutdown();
                    }
                    tunnel_relay::Input::Packet { bs } => {
                        let mut src = BytesBinarySource::new(&bs);
                        let item = src.packed::<_, _Any, _>(
                            &mut ActivatedMembranes(t, &self.self_ref, &mut self.membranes))
                            .demand_next(false)?;
                        tracing::trace!(packet = debug(&item), "-->");
                        self.handle_inbound_packet(t, Packet::try_from(&item)?)?
                    }
                    tunnel_relay::Input::Segment { bs } => {
                        self.input_buffer.extend_from_slice(&bs);
                        loop {
                            let (e, count) = {
                                let mut src = BytesBinarySource::new(&self.input_buffer);
                                let mut dec = ActivatedMembranes(t, &self.self_ref, &mut self.membranes);
                                let mut r = src.packed::<_, _Any, _>(&mut dec);
                                let e = r.next(false)?;
                                (e, r.source.index)
                            };
                            match e {
                                None => break,
                                Some(item) => {
                                    self.input_buffer.advance(count);
                                    tracing::trace!(packet = debug(&item), "-->");
                                    self.handle_inbound_packet(t, Packet::try_from(&item)?)?;
                                }
                            }
                        }
                    }
                }
                tunnel_relay::RelayProtocol::Output(b) => match *b {
                    tunnel_relay::Output { oid, event } => {
                        if self.pending_outbound.is_empty() {
                            t.message_immediate_self(
                                &self.self_ref, &tunnel_relay::RelayProtocol::Flush);
                        }
                        let turn_event = TurnEvent {
                            oid: Oid(oid.0),
                            event: self.handle_outbound_event(t, event)?,
                        };
                        self.pending_outbound.push(turn_event);
                    }
                }
                tunnel_relay::RelayProtocol::SyncGc(b) => match *b {
                    tunnel_relay::SyncGc { peer } => {
                        if let Some(ws) = self.membranes.imported.ref_map.get(&peer) {
                            let ws = Arc::clone(ws); // cloned to release the borrow to permit the release
                            self.membranes.imported.release(&ws);
                        }
                    }
                }
                tunnel_relay::RelayProtocol::Flush => {
                    let events = std::mem::take(&mut self.pending_outbound);
                    self.send_packet(Packet::Turn(Box::new(Turn(events))))?
                }
            }
        }
        Ok(())
    }

    fn exit_hook(&mut self, _t: &mut Activation, exit_status: &ActorResult) -> BoxFuture<ActorResult> {
        if let Err(e) = exit_status {
            let e = e.clone();
            Box::pin(ready(self.send_packet(Packet::Error(Box::new(e)))))
        } else {
            Box::pin(ready(Ok(())))
        }
    }
}

impl Entity for RelayEntity {
    fn assert(&mut self, t: &mut Activation, a: _Any, h: Handle) -> ActorResult {
        Ok(t.message(&self.relay_ref, &tunnel_relay::Output {
            oid: self.oid.clone(),
            event: Event::Assert(Box::new(Assert { assertion: Assertion(a), handle: h })),
        }))
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        Ok(t.message(&self.relay_ref, &tunnel_relay::Output {
            oid: self.oid.clone(),
            event: Event::Retract(Box::new(Retract { handle: h })),
        }))
    }
    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        Ok(t.message(&self.relay_ref, &tunnel_relay::Output {
            oid: self.oid.clone(),
            event: Event::Message(Box::new(Message { body: Assertion(m) })),
        }))
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref>) -> ActorResult {
        Ok(t.message(&self.relay_ref, &tunnel_relay::Output {
            oid: self.oid.clone(),
            event: Event::Sync(Box::new(Sync { peer })),
        }))
    }
}
