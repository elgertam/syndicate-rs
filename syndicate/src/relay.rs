use bytes::Buf;
use bytes::BytesMut;

use crate::actor::*;
use crate::during;
use crate::error::Error;
use crate::error::error;
use crate::schemas::gatekeeper;
use crate::schemas::internal_protocol as P;
use crate::schemas::sturdy;

use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;

use preserves::error::Error as PreservesError;
use preserves::error::is_eof_io_error;
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
use preserves::value::TextWriter;
use preserves::value::ViaCodec;
use preserves::value::Writer;
use preserves::value::signed_integer::SignedInteger;

use preserves_schema::support::Deserialize;
use preserves_schema::support::ParseError;

use std::convert::TryFrom;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};

struct WireSymbol {
    oid: sturdy::Oid,
    obj: Arc<Cap>,
    ref_count: AtomicUsize,
}

struct Membrane {
    oid_map: Map<sturdy::Oid, Arc<WireSymbol>>,
    ref_map: Map<Arc<Cap>, Arc<WireSymbol>>,
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

type TunnelRelayRef = Arc<Mutex<Option<TunnelRelay>>>;

// There are other kinds of relay. This one has exactly two participants connected to each other.
pub struct TunnelRelay
{
    self_ref: TunnelRelayRef,
    inbound_assertions: Map</* remote */ P::Handle, (/* local */ Handle, Vec<Arc<WireSymbol>>)>,
    outbound_assertions: Map<P::Handle, Vec<Arc<WireSymbol>>>,
    membranes: Membranes,
    pending_outbound: Vec<P::TurnEvent>,
    self_entity: Arc<Ref<()>>,
    output: UnboundedSender<LoanedItem<Vec<u8>>>,
    output_text: bool,
}

struct RelayEntity {
    relay_ref: TunnelRelayRef,
    oid: sturdy::Oid,
}

struct TunnelRefEntity {
    relay_ref: TunnelRelayRef,
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

    fn insert(&mut self, oid: sturdy::Oid, obj: Arc<Cap>) -> Arc<WireSymbol> {
        let ws = Arc::new(WireSymbol {
            oid: oid.clone(),
            obj: Arc::clone(&obj),
            ref_count: AtomicUsize::new(0),
        });
        self.oid_map.insert(oid, Arc::clone(&ws));
        self.ref_map.insert(obj, Arc::clone(&ws));
        ws
    }

    fn acquire(&mut self, r: &Arc<Cap>) -> Arc<WireSymbol> {
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

pub fn connect_stream<I, O, E, F>(
    t: &mut Activation,
    i: I,
    o: O,
    sturdyref: sturdy::SturdyRef,
    initial_state: E,
    mut f: F,
) where
    I: 'static + Send + AsyncRead,
    O: 'static + Send + AsyncWrite,
    E: 'static + Send,
    F: 'static + Send + FnMut(&mut E, &mut Activation, Arc<Cap>) -> during::DuringResult<E>
{
    let i = Input::Bytes(Box::pin(i));
    let o = Output::Bytes(Box::pin(o));
    let gatekeeper = TunnelRelay::run(t, i, o, None, Some(sturdy::Oid(0.into()))).unwrap();
    let main_entity = t.state.create(during::entity(initial_state).on_asserted(move |state, t, a: AnyValue| {
        let denotation = a.value().to_embedded()?;
        f(state, t, Arc::clone(denotation))
    }));
    gatekeeper.assert(t, &gatekeeper::Resolve {
        sturdyref,
        observer: Cap::new(&main_entity),
    });
}

impl TunnelRelay {
    pub fn run(
        t: &mut Activation,
        i: Input,
        o: Output,
        initial_ref: Option<Arc<Cap>>,
        initial_oid: Option<sturdy::Oid>,
    ) -> Option<Arc<Cap>> {
        let (output_tx, output_rx) = unbounded_channel();
        let tr_ref = Arc::new(Mutex::new(None));
        let self_entity = t.state.create(TunnelRefEntity {
            relay_ref: Arc::clone(&tr_ref),
        });
        let mut tr = TunnelRelay {
            self_ref: Arc::clone(&tr_ref),
            output: output_tx,
            output_text: false,
            inbound_assertions: Map::new(),
            outbound_assertions: Map::new(),
            membranes: Membranes {
                exported: Membrane::new(),
                imported: Membrane::new(),
                next_export_oid: 0,
            },
            pending_outbound: Vec::new(),
            self_entity: self_entity.clone(),
        };
        if let Some(ir) = initial_ref {
            tr.membranes.export_ref(ir, true);
        }
        let result = initial_oid.map(
            |io| Arc::clone(&tr.membranes.import_oid(t.state, &tr_ref, io).obj));
        *tr_ref.lock().unwrap() = Some(tr);
        t.state.linked_task(crate::name!("writer"), output_loop(o, output_rx));
        t.state.linked_task(crate::name!("reader"), input_loop(t.actor.clone(), i, tr_ref));
        t.state.add_exit_hook(&self_entity);
        result
    }

    fn deserialize_one(&mut self, t: &mut Activation, bs: &[u8]) -> (Result<P::Packet, ParseError>, usize) {
        let mut src = BytesBinarySource::new(&bs);
        let mut dec = ActivatedMembranes(t, &self.self_ref, &mut self.membranes);
        match src.peek() {
            Ok(v) => if v >= 128 {
                self.output_text = false;
                let mut r = src.packed::<_, AnyValue, _>(&mut dec);
                let res = P::Packet::deserialize(&mut r);
                (res, r.source.index)
            } else {
                self.output_text = true;
                let mut dec = ViaCodec::new(dec);
                let mut r = src.text::<_, AnyValue, _>(&mut dec);
                let res = P::Packet::deserialize(&mut r);
                (res, r.source.index)
            },
            Err(e) => (Err(e.into()), 0)
        }
    }

    fn handle_inbound_datagram(&mut self, t: &mut Activation, bs: &[u8]) -> ActorResult {
        let item = self.deserialize_one(t, bs).0?;
        self.handle_inbound_packet(t, item)
    }

    fn handle_inbound_stream(&mut self, t: &mut Activation, buf: &mut BytesMut) -> ActorResult {
        loop {
            let (result, count) = self.deserialize_one(t, buf);
            match result {
                Err(ParseError::Preserves(PreservesError::Io(e)))
                    if is_eof_io_error(&e) => return Ok(()),
                Err(e) => return Err(e)?,
                Ok(item) => {
                    buf.advance(count);
                    self.handle_inbound_packet(t, item)?;
                }
            }
        }
    }

    fn handle_inbound_packet(&mut self, t: &mut Activation, p: P::Packet) -> ActorResult {
        // tracing::trace!(packet = debug(&p), "-->");
        match p {
            P::Packet::Error(b) => {
                tracing::info!(message = debug(b.message.clone()),
                               detail = debug(b.detail.clone()),
                               "received Error from peer");
                Err(*b)
            },
            P::Packet::Turn(b) => {
                let P::Turn(events) = *b;
                for P::TurnEvent { oid, event } in events {
                    let target = match self.membranes.exported.oid_map.get(&sturdy::Oid(oid.0.clone())) {
                        Some(ws) => &ws.obj,
                        None => return Err(error("Cannot deliver event: nonexistent oid",
                                                 AnyValue::from(&P::TurnEvent { oid, event }))),
                    };
                    match event {
                        P::Event::Assert(b) => {
                            let P::Assert { assertion: P::Assertion(a), handle: remote_handle } = *b;
                            let mut imported = vec![];
                            let imported_membrane = &mut self.membranes.imported;
                            a.foreach_embedded::<_, Error>(&mut |r| {
                                Ok(imported.push(imported_membrane.acquire(r)))
                            })?;
                            if let Some(local_handle) = target.assert(t, a) {
                                if let Some(_) = self.inbound_assertions.insert(remote_handle, (local_handle, imported)) {
                                    return Err(error("Assertion with duplicate handle", AnyValue::new(false)));
                                }
                            }
                        }
                        P::Event::Retract(b) => {
                            let P::Retract { handle: remote_handle } = *b;
                            let (local_handle, imported) = match self.inbound_assertions.remove(&remote_handle) {
                                None => return Err(error("Retraction of nonexistent handle", AnyValue::from(&remote_handle))),
                                Some(wss) => wss,
                            };
                            for ws in imported.into_iter() {
                                self.membranes.imported.release(&ws);
                            }
                            t.retract(local_handle);
                        }
                        P::Event::Message(b) => {
                            let P::Message { body: P::Assertion(a) } = *b;
                            let imported_membrane = &mut self.membranes.imported;
                            a.foreach_embedded(&mut |r| {
                                let ws = imported_membrane.acquire(r);
                                match ws.ref_count.load(Ordering::SeqCst) {
                                    1 => Err(error("Cannot receive transient reference", AnyValue::new(false))),
                                    _ => Ok(())
                                }
                            })?;
                            target.message(t, a);
                        }
                        P::Event::Sync(b) => {
                            let P::Sync { peer } = *b;
                            self.membranes.imported.acquire(&peer);
                            struct SyncPeer {
                                relay_ref: TunnelRelayRef,
                                peer: Arc<Cap>,
                            }
                            impl Entity<Synced> for SyncPeer {
                                fn message(&mut self, t: &mut Activation, _a: Synced) -> ActorResult {
                                    self.peer.message(t, AnyValue::new(true));
                                    let mut g = self.relay_ref.lock().expect("unpoisoned");
                                    let tr = g.as_mut().expect("initialized");
                                    if let Some(ws) = tr.membranes.imported.ref_map.get(&self.peer) {
                                        let ws = Arc::clone(ws); // cloned to release the borrow to permit the release
                                        tr.membranes.imported.release(&ws);
                                    }
                                    Ok(())
                                }
                            }
                            let k = t.state.create(SyncPeer {
                                relay_ref: Arc::clone(&self.self_ref),
                                peer: Arc::clone(&peer),
                            });
                            t.sync(&peer.underlying, k);
                        }
                    }
                }
                t.deliver();
                Ok(())
            }
        }
    }

    fn handle_outbound_event(&mut self, t: &mut Activation, event: P::Event) -> Result<P::Event, Error> {
        match &event {
            P::Event::Assert(b) => {
                let P::Assert { assertion: P::Assertion(a), handle } = &**b;
                let mut outbound = Vec::new();
                a.foreach_embedded::<_, Error>(
                    &mut |r| Ok(outbound.push(self.membranes.export_ref(Arc::clone(r), true))))?;
                self.outbound_assertions.insert(handle.clone(), outbound);
            }
            P::Event::Retract(b) => {
                let P::Retract { handle } = &**b;
                if let Some(outbound) = self.outbound_assertions.remove(handle) {
                    for ws in outbound.into_iter() {
                        self.membranes.exported.release(&ws);
                    }
                }
            }
            P::Event::Message(b) => {
                let P::Message { body: P::Assertion(a) } = &**b;
                a.foreach_embedded(&mut |r| {
                    let ws = self.membranes.export_ref(Arc::clone(r), false);
                    match ws.ref_count.load(Ordering::SeqCst) {
                        0 => Err(error("Cannot send transient reference", AnyValue::new(false))),
                        _ => Ok(())
                    }
                })?;
            },
            P::Event::Sync(_b) => panic!("TODO not yet implemented"),
        }
        Ok(event)
    }

    fn encode_packet(&mut self, p: P::Packet) -> Result<Vec<u8>, Error> {
        let item = AnyValue::from(&p);
        // tracing::trace!(packet = debug(&item), "<--");
        if self.output_text {
            let mut s = TextWriter::encode::<_, AnyValue, _>(&mut self.membranes, &item)?;
            s.push('\n');
            Ok(s.into_bytes())
        } else {
            Ok(PackedWriter::encode::<_, AnyValue, _>(&mut self.membranes, &item)?)
        }
    }

    pub fn send_packet(&mut self, debtor: &Arc<Debtor>, cost: usize, p: P::Packet) -> ActorResult {
        let bs = self.encode_packet(p)?;
        let _ = self.output.send(LoanedItem::new(debtor, cost, bs));
        Ok(())
    }

    pub fn send_event(&mut self, t: &mut Activation, oid: sturdy::Oid, event: P::Event) -> ActorResult {
        if self.pending_outbound.is_empty() {
            t.message_for_myself(&self.self_entity, ());
        }
        let turn_event = P::TurnEvent {
            oid: P::Oid(oid.0),
            event: self.handle_outbound_event(t, event)?,
        };
        self.pending_outbound.push(turn_event);
        Ok(())
    }
}

impl Membranes {
    fn export_ref(&mut self, obj: Arc<Cap>, and_acquire: bool) -> Arc<WireSymbol> {
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
        ac: &mut RunningActor,
        relay_ref: &TunnelRelayRef,
        oid: sturdy::Oid,
    ) -> Arc<WireSymbol> {
        let obj = ac.create(RelayEntity { relay_ref: Arc::clone(relay_ref), oid: oid.clone() });
        self.imported.insert(oid, Cap::new(&obj))
    }

    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        t: &mut Activation,
        relay_ref: &TunnelRelayRef,
        src: &'src mut S,
        _read_annotations: bool,
    ) -> io::Result<P::_Ptr> {
        let v: IOValue = PackedReader::new(src, NoEmbeddedDomainCodec).demand_next(false)?;
        match sturdy::WireRef::try_from(&v)? {
            sturdy::WireRef::Mine{ oid: b } => {
                let oid = *b;
                match self.imported.oid_map.get(&oid) {
                    Some(ws) => Ok(Arc::clone(&ws.obj)),
                    None => Ok(Arc::clone(&self.import_oid(t.state, relay_ref, oid).obj)),
                }
            }
            sturdy::WireRef::Yours { oid: b, attenuation } => {
                let oid = *b;
                match self.exported.oid_map.get(&oid) {
                    Some(ws) => {
                        if attenuation.is_empty() {
                            Ok(Arc::clone(&ws.obj))
                        } else {
                            Ok(ws.obj.attenuate(&sturdy::Attenuation(attenuation))
                               .map_err(|e| {
                                   io::Error::new(
                                       io::ErrorKind::InvalidInput,
                                       format!("Invalid capability attenuation: {:?}", e))
                               })?)
                        }
                    }
                    None => Ok(Cap::new(&t.state.inert_entity())),
                }
            }
        }
    }
}

struct ActivatedMembranes<'a, 'activation, 'm>(&'a mut Activation<'activation>,
                                               &'m TunnelRelayRef,
                                               &'m mut Membranes);

impl<'a, 'activation, 'm> DomainDecode<P::_Ptr> for ActivatedMembranes<'a, 'activation, 'm> {
    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        src: &'src mut S,
        read_annotations: bool,
    ) -> io::Result<P::_Ptr> {
        self.2.decode_embedded(self.0, self.1, src, read_annotations)
    }
}

impl DomainEncode<P::_Ptr> for Membranes {
    fn encode_embedded<W: Writer>(
        &mut self,
        w: &mut W,
        d: &P::_Ptr,
    ) -> io::Result<()> {
        w.write(&mut NoEmbeddedDomainCodec, &AnyValue::from(&match self.exported.ref_map.get(d) {
            Some(ws) => sturdy::WireRef::Mine {
                oid: Box::new(ws.oid.clone()),
            },
            None => match self.imported.ref_map.get(d) {
                Some(ws) => {
                    if d.attenuation.is_empty() {
                        sturdy::WireRef::Yours {
                            oid: Box::new(ws.oid.clone()),
                            attenuation: vec![],
                        }
                    } else {
                        // We may trust the peer to enforce attenuation on our behalf, in
                        // which case we can return sturdy::WireRef::Yours with an attenuation
                        // attached here, but for now we don't.
                        sturdy::WireRef::Mine {
                            oid: Box::new(self.export_ref(Arc::clone(d), false).oid.clone()),
                        }
                    }
                }
                None =>
                    sturdy::WireRef::Mine {
                        oid: Box::new(self.export_ref(Arc::clone(d), false).oid.clone()),
                    },
            }
        }))
    }
}

async fn input_loop(
    ac: ActorRef,
    i: Input,
    relay: TunnelRelayRef,
) -> ActorResult {
    let debtor = Debtor::new(crate::name!("input-loop"));
    match i {
        Input::Packets(mut src) => {
            loop {
                debtor.ensure_clear_funds().await;
                match src.next().await {
                    None => return Activation::for_actor(&ac, Arc::clone(&debtor), |t| {
                        Ok(t.state.shutdown())
                    }),
                    Some(bs) => Activation::for_actor(&ac, Arc::clone(&debtor), |t| {
                        let mut g = relay.lock().expect("unpoisoned");
                        let tr = g.as_mut().expect("initialized");
                        tr.handle_inbound_datagram(t, &bs?)
                    })?,
                }
            }
        }
        Input::Bytes(mut r) => {
            const BUFSIZE: usize = 65536;
            let mut buf = BytesMut::with_capacity(BUFSIZE);
            loop {
                debtor.ensure_clear_funds().await;
                buf.reserve(BUFSIZE);
                let n = match r.read_buf(&mut buf).await {
                    Ok(n) => n,
                    Err(e) =>
                        if e.kind() == io::ErrorKind::ConnectionReset {
                            return Activation::for_actor(&ac, Arc::clone(&debtor), |t| {
                                Ok(t.state.shutdown())
                            });
                        } else {
                            return Err(e)?;
                        },
                };
                match n {
                    0 => return Activation::for_actor(&ac, Arc::clone(&debtor), |t| {
                        Ok(t.state.shutdown())
                    }),
                    _ => Activation::for_actor(&ac, Arc::clone(&debtor), |t| {
                        let mut g = relay.lock().expect("unpoisoned");
                        let tr = g.as_mut().expect("initialized");
                        tr.handle_inbound_stream(t, &mut buf)
                    })?,
                }
            }
        }
    }
}

async fn output_loop(
    mut o: Output,
    mut output_rx: UnboundedReceiver<LoanedItem<Vec<u8>>>,
) -> ActorResult {
    loop {
        match output_rx.recv().await {
            None =>
                return Ok(()),
            Some(mut loaned_item) => {
                match &mut o {
                    Output::Packets(sink) => sink.send(std::mem::take(&mut loaned_item.item)).await?,
                    Output::Bytes(w) => {
                        w.write_all(&loaned_item.item).await?;
                        w.flush().await?;
                    }
                }
            }
        }
    }
}

impl Entity<()> for TunnelRefEntity {
    fn message(&mut self, t: &mut Activation, _m: ()) -> ActorResult {
        let mut g = self.relay_ref.lock().expect("unpoisoned");
        let tr = g.as_mut().expect("initialized");
        let events = std::mem::take(&mut tr.pending_outbound);
        tr.send_packet(&t.debtor(), events.len(), P::Packet::Turn(Box::new(P::Turn(events))))
    }

    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        if let Err(e) = &**exit_status {
            let e = e.clone();
            let mut g = self.relay_ref.lock().expect("unpoisoned");
            let tr = g.as_mut().expect("initialized");
            tr.send_packet(&t.debtor(), 1, P::Packet::Error(Box::new(e)))?;
        }
        Ok(())
    }
}

impl Entity<AnyValue> for RelayEntity {
    fn assert(&mut self, t: &mut Activation, a: AnyValue, h: Handle) -> ActorResult {
        let mut g = self.relay_ref.lock().expect("unpoisoned");
        let tr = g.as_mut().expect("initialized");
        tr.send_event(t, self.oid.clone(), P::Event::Assert(Box::new(P::Assert {
            assertion: P::Assertion(a),
            handle: P::Handle(h.into()),
        })))
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        let mut g = self.relay_ref.lock().expect("unpoisoned");
        let tr = g.as_mut().expect("initialized");
        tr.send_event(t, self.oid.clone(), P::Event::Retract(Box::new(P::Retract {
            handle: P::Handle(h.into()),
        })))
    }
    fn message(&mut self, t: &mut Activation, m: AnyValue) -> ActorResult {
        let mut g = self.relay_ref.lock().expect("unpoisoned");
        let tr = g.as_mut().expect("initialized");
        tr.send_event(t, self.oid.clone(), P::Event::Message(Box::new(P::Message {
            body: P::Assertion(m)
        })))
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        let mut g = self.relay_ref.lock().expect("unpoisoned");
        let tr = g.as_mut().expect("initialized");
        tr.send_event(t, self.oid.clone(), P::Event::Sync(Box::new(P::Sync {
            peer: Cap::guard(&peer)
        })))
    }
}
