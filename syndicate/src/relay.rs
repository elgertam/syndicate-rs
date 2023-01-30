use bytes::Buf;
use bytes::BytesMut;

use crate::language;
use crate::actor::*;
use crate::during;
use crate::error::Error;
use crate::error::error;
use crate::schemas::gatekeeper;
use crate::schemas::protocol as P;
use crate::schemas::sturdy;
use crate::trace;

use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;

pub use parking_lot::Mutex;

use preserves::error::Error as PreservesError;
use preserves::error::is_eof_io_error;
use preserves::value::BinarySource;
use preserves::value::BytesBinarySource;
use preserves::value::DomainDecode;
use preserves::value::DomainEncode;
use preserves::value::Map;
use preserves::value::NestedValue;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::PackedWriter;
use preserves::value::TextWriter;
use preserves::value::ViaCodec;
use preserves::value::Writer;
use preserves::value::signed_integer::SignedInteger;

use preserves_schema::Codec;
use preserves_schema::Deserialize;
use preserves_schema::ParseError;

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

#[derive(Debug, Clone, Copy)]
enum WireSymbolSide {
    Imported,
    Exported,
}

struct WireSymbol {
    oid: sturdy::Oid,
    obj: Arc<Cap>,
    ref_count: AtomicUsize,
    side: WireSymbolSide,
}

struct Membrane {
    side: WireSymbolSide,
    oid_map: Map<sturdy::Oid, Arc<WireSymbol>>,
    ref_map: Map<Arc<Cap>, Arc<WireSymbol>>,
}

#[derive(Debug)]
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
#[derive(Debug)]
pub struct TunnelRelay
{
    self_ref: TunnelRelayRef,
    inbound_assertions: Map</* remote */ P::Handle, (/* local */ Handle, Vec<Arc<WireSymbol>>)>,
    outbound_assertions: Map<P::Handle, Vec<Arc<WireSymbol>>>,
    membranes: Membranes,
    pending_outbound: Vec<P::TurnEvent<AnyValue>>,
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

struct ActivatedMembranes<'a, 'activation, 'm> {
    turn: &'a mut Activation<'activation>,
    tr_ref: &'m TunnelRelayRef,
    membranes: &'m mut Membranes,
}

//---------------------------------------------------------------------------

impl WireSymbol {
    #[inline]
    fn inc_ref<'a>(self: &'a Arc<Self>) -> &'a Arc<Self> {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        tracing::trace!(?self, "after inc_ref");
        self
    }

    #[inline]
    fn dec_ref(&self) -> bool {
        let old_count = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        tracing::trace!(?self, "after dec_ref");
        old_count == 1
    }

    #[inline]
    fn current_ref_count(&self) -> usize {
        self.ref_count.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for WireSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "#<WireSymbol oid={:?}:{} obj={:?} ref_count={}>",
               self.side,
               self.oid.0,
               self.obj,
               self.current_ref_count())
    }
}

impl Membrane {
    fn new(side: WireSymbolSide) -> Self {
        Membrane {
            side,
            oid_map: Map::new(),
            ref_map: Map::new(),
        }
    }

    fn insert(&mut self, oid: sturdy::Oid, obj: Arc<Cap>) -> Arc<WireSymbol> {
        let ws = Arc::new(WireSymbol {
            oid: oid.clone(),
            obj: Arc::clone(&obj),
            ref_count: AtomicUsize::new(0),
            side: self.side,
        });
        self.oid_map.insert(oid, Arc::clone(&ws));
        self.ref_map.insert(obj, Arc::clone(&ws));
        ws
    }

    fn insert_inert_entity(&mut self, t: &mut Activation, oid: sturdy::Oid) -> Arc<WireSymbol> {
        self.insert(oid, Cap::new(&t.inert_entity()))
    }
}

pub fn connect_stream<I, O, E, F>(
    t: &mut Activation,
    i: I,
    o: O,
    output_text: bool,
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
    let gatekeeper = TunnelRelay::run(t, i, o, None, Some(sturdy::Oid(0.into())), output_text).unwrap();
    let main_entity = t.create(during::entity(initial_state).on_asserted(move |state, t, a: AnyValue| {
        let denotation = a.value().to_embedded()?;
        f(state, t, Arc::clone(denotation))
    }));
    gatekeeper.assert(t, language(), &gatekeeper::Resolve {
        sturdyref,
        observer: Cap::new(&main_entity),
    });
}

impl std::fmt::Debug for Membrane {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.debug_struct("Membrane")
            .field("side", &self.side)
            .field("refs", &self.oid_map.values())
            .finish()
    }
}

macro_rules! dump_membranes { ($e:expr) => { tracing::trace!("membranes: {:#?}", $e); } }
// macro_rules! dump_membranes { ($e:expr) => { (); } }

impl TunnelRelay {
    pub fn run(
        t: &mut Activation,
        i: Input,
        o: Output,
        initial_ref: Option<Arc<Cap>>,
        initial_oid: Option<sturdy::Oid>,
        output_text: bool,
    ) -> Option<Arc<Cap>> {
        let (result, tr_ref, output_rx) = TunnelRelay::_run(t, initial_ref, initial_oid, output_text);
        t.linked_task(Some(AnyValue::symbol("writer")),
                      output_loop(o, output_rx));
        t.linked_task(Some(AnyValue::symbol("reader")),
                      input_loop(t.trace_collector(), t.facet.clone(), i, tr_ref));
        result
    }

    pub fn _run(
        t: &mut Activation,
        initial_ref: Option<Arc<Cap>>,
        initial_oid: Option<sturdy::Oid>,
        output_text: bool,
    ) -> (Option<Arc<Cap>>, Arc<Mutex<Option<TunnelRelay>>>, UnboundedReceiver<LoanedItem<Vec<u8>>>) {
        let (output_tx, output_rx) = unbounded_channel();
        let tr_ref = Arc::new(Mutex::new(None));
        let self_entity = t.create(TunnelRefEntity {
            relay_ref: Arc::clone(&tr_ref),
        });
        let mut tr = TunnelRelay {
            self_ref: Arc::clone(&tr_ref),
            output: output_tx,
            output_text,
            inbound_assertions: Map::new(),
            outbound_assertions: Map::new(),
            membranes: Membranes {
                exported: Membrane::new(WireSymbolSide::Exported),
                imported: Membrane::new(WireSymbolSide::Imported),
                next_export_oid: 0,
            },
            pending_outbound: Vec::new(),
        };
        if let Some(ir) = initial_ref {
            tr.membranes.export_ref(ir).inc_ref();
        }
        let result = initial_oid.map(
            |io| Arc::clone(&tr.membranes.import_oid(t, &tr_ref, io).inc_ref().obj));
        dump_membranes!(tr.membranes);
        *tr_ref.lock() = Some(tr);
        t.state.add_exit_hook(&self_entity);
        (result, tr_ref, output_rx)
    }

    fn deserialize_one(&mut self, t: &mut Activation, bs: &[u8]) -> (Result<P::Packet<AnyValue>, ParseError>, usize) {
        let mut src = BytesBinarySource::new(&bs);
        let mut dec = ActivatedMembranes {
            turn: t,
            tr_ref: &self.self_ref,
            membranes: &mut self.membranes,
        };
        match src.peek() {
            Ok(v) => if v >= 128 {
                self.output_text = false;
                let mut r = src.packed(&mut dec);
                let res = P::Packet::deserialize(&mut r);
                (res, r.source.index)
            } else {
                self.output_text = true;
                let mut dec = ViaCodec::new(dec);
                let mut r = src.text::<AnyValue, _>(&mut dec);
                let res = P::Packet::deserialize(&mut r);
                (res, r.source.index)
            },
            Err(e) => (Err(e.into()), 0)
        }
    }

    pub fn handle_inbound_datagram(&mut self, t: &mut Activation, bs: &[u8]) -> ActorResult {
        tracing::trace!(bytes = ?bs, "inbound datagram");
        let item = self.deserialize_one(t, bs).0?;
        self.handle_inbound_packet(t, item)
    }

    pub fn handle_inbound_stream(&mut self, t: &mut Activation, buf: &mut BytesMut) -> ActorResult {
        loop {
            tracing::trace!(buffer = ?buf, "inbound stream");
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

    pub fn handle_inbound_packet(&mut self, t: &mut Activation, p: P::Packet<AnyValue>) -> ActorResult {
        tracing::debug!(packet = ?p, "-->");
        match p {
            P::Packet::Extension(b) => {
                let P::Extension { label, fields } = *b;
                tracing::info!(?label, ?fields, "received Extension from peer");
                Ok(())
            }
            P::Packet::Error(b) => {
                tracing::info!(message = ?b.message.clone(),
                               detail = ?b.detail.clone(),
                               "received Error from peer");
                Err(*b)?
            }
            P::Packet::Turn(b) => {
                let P::Turn(events) = *b;
                for P::TurnEvent { oid, event } in events {
                    tracing::trace!(?oid, ?event, "handle_inbound");
                    let target = match self.membranes.exported.oid_map.get(&sturdy::Oid(oid.0.clone())) {
                        Some(ws) =>
                            ws.inc_ref(),
                        None => {
                            tracing::debug!(
                                event = ?language().unparse(&P::TurnEvent { oid, event }),
                                "Cannot deliver event: nonexistent oid");
                            continue;
                        }
                    };
                    let mut pins = vec![target.clone()];
                    let target = Arc::clone(&target.obj);
                    match event {
                        P::Event::Assert(b) => {
                            let P::Assert { assertion: P::Assertion(a), handle: remote_handle } = *b;
                            a.foreach_embedded::<_, Error>(
                                &mut |r| Ok(pins.push(self.membranes.lookup_ref(r))))?;
                            if let Some(local_handle) = target.assert(t, &(), &a) {
                                if let Some(_) = self.inbound_assertions.insert(remote_handle, (local_handle, pins)) {
                                    return Err(error("Assertion with duplicate handle", AnyValue::new(false)))?;
                                }
                            } else {
                                self.membranes.release(pins);
                            }
                            dump_membranes!(self.membranes);
                        }
                        P::Event::Retract(b) => {
                            let P::Retract { handle: remote_handle } = *b;
                            let (local_handle, previous_pins) = match self.inbound_assertions.remove(&remote_handle) {
                                None => return Err(error("Retraction of nonexistent handle", language().unparse(&remote_handle)))?,
                                Some(wss) => wss,
                            };
                            self.membranes.release(previous_pins);
                            self.membranes.release(pins);
                            t.retract(local_handle);
                            dump_membranes!(self.membranes);
                        }
                        P::Event::Message(b) => {
                            let P::Message { body: P::Assertion(a) } = *b;
                            a.foreach_embedded(&mut |r| {
                                let ws = self.membranes.lookup_ref(r);
                                let rc = ws.current_ref_count();
                                pins.push(ws);
                                match rc {
                                    1 => Err(error("Cannot receive transient reference", AnyValue::new(false))),
                                    _ => Ok(())
                                }
                            })?;
                            target.message(t, &(), &a);
                            self.membranes.release(pins);
                            dump_membranes!(self.membranes);
                        }
                        P::Event::Sync(b) => {
                            let P::Sync { peer } = *b;
                            pins.push(self.membranes.lookup_ref(&peer));
                            dump_membranes!(self.membranes);
                            struct SyncPeer {
                                relay_ref: TunnelRelayRef,
                                peer: Arc<Cap>,
                                pins: Vec<Arc<WireSymbol>>,
                            }
                            impl Entity<Synced> for SyncPeer {
                                fn message(&mut self, t: &mut Activation, _a: Synced) -> ActorResult {
                                    self.peer.message(t, &(), &AnyValue::new(true));
                                    let mut g = self.relay_ref.lock();
                                    let tr = g.as_mut().expect("initialized");
                                    tr.membranes.release(std::mem::take(&mut self.pins));
                                    dump_membranes!(tr.membranes);
                                    Ok(())
                                }
                            }
                            let k = t.create(SyncPeer {
                                relay_ref: Arc::clone(&self.self_ref),
                                peer: Arc::clone(&peer),
                                pins,
                            });
                            target.sync(t, k);
                        }
                    }
                }
                t.commit()
            }
        }
    }

    fn outbound_event_bookkeeping(
        &mut self,
        _t: &mut Activation,
        remote_oid: sturdy::Oid,
        event: &P::Event<AnyValue>,
    ) -> ActorResult {
        match event {
            P::Event::Assert(b) => {
                let P::Assert { assertion: P::Assertion(a), handle } = &**b;
                if let Some(target_ws) = self.membranes.imported.oid_map.get(&remote_oid).map(Arc::clone) {
                    target_ws.inc_ref(); // encoding won't do this; target oid is syntactically special
                    let mut pins = vec![target_ws];
                    a.foreach_embedded::<_, Error>(
                        &mut |r| Ok(pins.push(self.membranes.lookup_ref(r))))?;
                    self.outbound_assertions.insert(handle.clone(), pins);
                    dump_membranes!(self.membranes);
                } else {
                    // This can happen if
                    // 1. remote peer asserts a value causing remote_oid to be allocated
                    // 2. some local actor holds a reference to that entity
                    // 3. remote peer retracts the value
                    // 4. local actor uses the ref
                    tracing::trace!(?remote_oid, "not registered in imported membrane (ok)");
                }
            }
            P::Event::Retract(b) => {
                let P::Retract { handle } = &**b;
                if let Some(pins) = self.outbound_assertions.remove(handle) {
                    self.membranes.release(pins);
                    dump_membranes!(self.membranes);
                } else {
                    // This can happen e.g. if an assert leads to no
                    // outbound message as in the scenario in the
                    // P::Event::Assert stanza above, and then the
                    // local actor retracts their assertion again.
                    tracing::trace!(?handle, "not registered in outbound_assertions (ok)");
                }
            }
            P::Event::Message(b) => {
                let P::Message { body: P::Assertion(a) } = &**b;
                a.foreach_embedded(&mut |r| {
                    let ws = self.membranes.lookup_ref(r);
                    if self.membranes.release_one(ws) { // undo the inc_ref from encoding
                        Err(error("Sent transient reference", AnyValue::new(false)))
                    } else {
                        Ok(())
                    }
                })?;
                dump_membranes!(self.membranes);
            },
            P::Event::Sync(_b) =>
                todo!(),
        }
        Ok(())
    }

    pub fn send_packet(&mut self, account: &Arc<Account>, cost: usize, p: P::Packet<AnyValue>) -> ActorResult {
        let item = language().unparse(&p);
        tracing::debug!(packet = ?item, "<--");

        let bs = if self.output_text {
            let mut s = TextWriter::encode(&mut self.membranes, &item)?;
            s.push('\n');
            s.into_bytes()
        } else {
            PackedWriter::encode(&mut self.membranes, &item)?
        };

        let _ = self.output.send(LoanedItem::new(account, cost, bs));
        Ok(())
    }

    pub fn send_event(&mut self, t: &mut Activation, remote_oid: sturdy::Oid, event: P::Event<AnyValue>) -> ActorResult {
        if self.pending_outbound.is_empty() {
            let self_ref = Arc::clone(&self.self_ref);
            t.pre_commit(move |t| {
                let mut g = self_ref.lock();
                let tr = g.as_mut().expect("initialized");
                let events = std::mem::take(&mut tr.pending_outbound);
                tr.send_packet(&t.account(),
                               events.len(),
                               P::Packet::Turn(Box::new(P::Turn(events.clone()))))?;
                for P::TurnEvent { oid, event } in events.into_iter() {
                    tr.outbound_event_bookkeeping(t, sturdy::Oid(oid.0), &event)?;
                }
                Ok(())
            });
        }
        self.pending_outbound.push(P::TurnEvent { oid: P::Oid(remote_oid.0), event });
        Ok(())
    }
}

impl Membranes {
    fn export_ref(&mut self, obj: Arc<Cap>) -> Arc<WireSymbol> {
        let oid = sturdy::Oid(SignedInteger::from(self.next_export_oid as u128));
        self.next_export_oid += 1;
        self.exported.insert(oid, obj)
    }

    fn import_oid(
        &mut self,
        t: &mut Activation,
        relay_ref: &TunnelRelayRef,
        oid: sturdy::Oid,
    ) -> Arc<WireSymbol> {
        let obj = t.create(RelayEntity { relay_ref: Arc::clone(relay_ref), oid: oid.clone() });
        self.imported.insert(oid, Cap::new(&obj))
    }

    #[inline]
    fn lookup_ref(&mut self, r: &Arc<Cap>) -> Arc<WireSymbol> {
        self.imported.ref_map.get(r).or_else(|| self.exported.ref_map.get(r)).map(Arc::clone)
            .expect("WireSymbol must be present at lookup_ref() time")
    }

    #[inline]
    fn membrane(&mut self, side: WireSymbolSide) -> &mut Membrane {
        match side {
            WireSymbolSide::Imported => &mut self.imported,
            WireSymbolSide::Exported => &mut self.exported,
        }
    }

    #[inline]
    fn release_one(&mut self, ws: Arc<WireSymbol>) -> bool {
        if ws.dec_ref() {
            let membrane = self.membrane(ws.side);
            membrane.oid_map.remove(&ws.oid);
            membrane.ref_map.remove(&ws.obj);
            true
        } else {
            false
        }
    }

    #[inline]
    fn release<I: IntoIterator<Item = Arc<WireSymbol>>>(&mut self, wss: I) {
        for ws in wss {
            self.release_one(ws);
        }
    }

    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        t: &mut Activation,
        relay_ref: &TunnelRelayRef,
        src: &'src mut S,
        _read_annotations: bool,
    ) -> io::Result<Arc<Cap>> {
        let ws = match sturdy::WireRef::deserialize(&mut src.packed(NoEmbeddedDomainCodec))? {
            sturdy::WireRef::Mine{ oid: b } => {
                let oid = *b;
                self.imported.oid_map.get(&oid).map(Arc::clone)
                    .unwrap_or_else(|| self.import_oid(t, relay_ref, oid))
            }
            sturdy::WireRef::Yours { oid: b, attenuation } => {
                let oid = *b;
                if attenuation.is_empty() {
                    self.exported.oid_map.get(&oid).map(Arc::clone).unwrap_or_else(
                        || self.exported.insert_inert_entity(t, oid))
                } else {
                    match self.exported.oid_map.get(&oid) {
                        None => self.exported.insert_inert_entity(t, oid),
                        Some(ws) => {
                            let attenuated_obj = ws.obj.attenuate(&sturdy::Attenuation(attenuation))
                                .map_err(|e| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("Invalid capability attenuation: {:?}", e))
                                })?;
                            self.exported.insert(oid, attenuated_obj)
                        }
                    }
                }
            }
        };
        Ok(Arc::clone(&ws.inc_ref().obj))
    }
}

impl<'a, 'activation, 'm> DomainDecode<Arc<Cap>> for ActivatedMembranes<'a, 'activation, 'm> {
    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        src: &'src mut S,
        read_annotations: bool,
    ) -> io::Result<Arc<Cap>> {
        self.membranes.decode_embedded(self.turn, self.tr_ref, src, read_annotations)
    }
}

impl DomainEncode<Arc<Cap>> for Membranes {
    fn encode_embedded<W: Writer>(
        &mut self,
        w: &mut W,
        d: &Arc<Cap>,
    ) -> io::Result<()> {
        w.write(&mut NoEmbeddedDomainCodec, &language().unparse(&match self.exported.ref_map.get(d) {
            Some(ws) => sturdy::WireRef::Mine {
                oid: Box::new(ws.inc_ref().oid.clone()),
            },
            None => match self.imported.ref_map.get(d) {
                Some(ws) => {
                    if d.attenuation.is_empty() {
                        sturdy::WireRef::Yours {
                            oid: Box::new(ws.inc_ref().oid.clone()),
                            attenuation: vec![],
                        }
                    } else {
                        // We may trust the peer to enforce attenuation on our behalf, in
                        // which case we can return sturdy::WireRef::Yours with an attenuation
                        // attached here, but for now we don't.
                        sturdy::WireRef::Mine {
                            oid: Box::new(self.export_ref(Arc::clone(d)).inc_ref().oid.clone()),
                        }
                    }
                }
                None =>
                    sturdy::WireRef::Mine {
                        oid: Box::new(self.export_ref(Arc::clone(d)).inc_ref().oid.clone()),
                    },
            }
        }))
    }
}

async fn input_loop(
    trace_collector: Option<trace::TraceCollector>,
    facet: FacetRef,
    i: Input,
    relay: TunnelRelayRef,
) -> Result<LinkedTaskTermination, Error> {
    let account = Account::new(Some(AnyValue::symbol("input-loop")), trace_collector);
    let cause = trace::TurnCause::external("input-loop");
    match i {
        Input::Packets(mut src) => {
            loop {
                account.ensure_clear_funds().await;
                match src.next().await {
                    None => break,
                    Some(bs) => {
                        if !facet.activate(
                            &account, Some(cause.clone()), |t| {
                                let mut g = relay.lock();
                                let tr = g.as_mut().expect("initialized");
                                tr.handle_inbound_datagram(t, &bs?)
                            })
                        {
                            break;
                        }
                    }
                }
            }
        }
        Input::Bytes(mut r) => {
            const BUFSIZE: usize = 65536;
            let mut buf = BytesMut::with_capacity(BUFSIZE);
            loop {
                account.ensure_clear_funds().await;
                buf.reserve(BUFSIZE);
                let n = match r.read_buf(&mut buf).await {
                    Ok(n) => n,
                    Err(e) =>
                        if e.kind() == io::ErrorKind::ConnectionReset {
                            break;
                        } else {
                            return Err(e)?;
                        },
                };
                match n {
                    0 => break,
                    _ => {
                        if !facet.activate(
                            &account, Some(cause.clone()), |t| {
                                let mut g = relay.lock();
                                let tr = g.as_mut().expect("initialized");
                                tr.handle_inbound_stream(t, &mut buf)
                            })
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(LinkedTaskTermination::Normal)
}

async fn output_loop(
    mut o: Output,
    mut output_rx: UnboundedReceiver<LoanedItem<Vec<u8>>>,
) -> Result<LinkedTaskTermination, Error> {
    loop {
        match output_rx.recv().await {
            None =>
                return Ok(LinkedTaskTermination::KeepFacet),
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
    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ExitStatus>) {
        if let Err(e) = &**exit_status {
            let e = e.clone();
            let mut g = self.relay_ref.lock();
            let tr = g.as_mut().expect("initialized");
            if let Err(f) = tr.send_packet(&t.account(), 1, P::Packet::Error(Box::new(e))) {
                tracing::error!("Failed to send error packet: {:?}", f);
            }
        }
    }
}

impl Entity<AnyValue> for RelayEntity {
    fn assert(&mut self, t: &mut Activation, a: AnyValue, h: Handle) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Assert(Box::new(P::Assert {
                assertion: P::Assertion(a),
                handle: P::Handle(h.into()),
            })))
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Retract(Box::new(P::Retract {
                handle: P::Handle(h.into()),
            })))
    }
    fn message(&mut self, t: &mut Activation, m: AnyValue) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Message(Box::new(P::Message {
                body: P::Assertion(m)
            })))
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Sync(Box::new(P::Sync {
                peer: Cap::guard(Arc::new(()), peer)
            })))
    }
}
