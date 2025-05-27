use bytes::Buf;
use bytes::BytesMut;

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

use preserves::BinarySource;
use preserves::BytesBinarySource;
use preserves::DomainDecode;
use preserves::DomainEncode;
use preserves::IOValue;
use preserves::IOValueReader;
use preserves::Map;
use preserves::PackedWriter;
use preserves::Reader;
use preserves::ReaderResult;
use preserves::Set;
use preserves::TextWriter;
use preserves::signed_integer::SignedInteger;
use preserves::value_map_embedded;

use preserves_schema::Deserialize;
use preserves_schema::Parse;
use preserves_schema::Unparse;

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
    oid: SignedInteger,
    obj: Arc<Cap>,
    ref_count: AtomicUsize,
    side: WireSymbolSide,
}

struct Membrane {
    side: WireSymbolSide,
    oid_map: Map<SignedInteger, Arc<WireSymbol>>,
    ref_map: Map<Arc<Cap>, Arc<WireSymbol>>,
}

#[derive(Debug)]
struct Membranes {
    exported: Membrane,
    imported: Membrane,
    next_export_oid: usize,
    reimported_attenuations: Map<SignedInteger, Set<Arc<Cap>>>,
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
    pending_outbound: Vec<P::TurnEvent<Arc<Cap>>>,
    output: UnboundedSender<LoanedItem<Vec<u8>>>,
    output_text: bool,
}

struct RelayEntity {
    relay_ref: TunnelRelayRef,
    oid: SignedInteger,
}

struct TunnelRefEntity {
    relay_ref: TunnelRelayRef,
}

struct ActivatedMembranes<'a, 'm> {
    turn: &'a mut Activation,
    tr_ref: &'m TunnelRelayRef,
    membranes: &'m mut Membranes,
    rollback_symbols: Vec<Arc<WireSymbol>>,
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
               self.oid,
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

    fn insert(&mut self, oid: SignedInteger, obj: Arc<Cap>) -> Arc<WireSymbol> {
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

    fn remove(&mut self, ws: &Arc<WireSymbol>) {
        self.oid_map.remove(&ws.oid);
        self.ref_map.remove(&ws.obj);
    }

    fn insert_inert_entity(&mut self, t: &mut Activation, oid: SignedInteger) -> Arc<WireSymbol> {
        self.insert(oid, Cap::new(&t.inert_entity()))
    }
}

pub fn connect_stream<I, O, Step, E, F>(
    t: &mut Activation,
    i: I,
    o: O,
    output_text: bool,
    step: Step,
    initial_state: E,
    mut f: F,
) -> ActorResult where
    I: 'static + Send + AsyncRead,
    O: 'static + Send + AsyncWrite,
    Step: Unparse<Arc<Cap>>,
    E: 'static + Send,
    F: 'static + Send + FnMut(&mut E, &mut Activation, Arc<Cap>) -> during::DuringResult<E>
{
    let i = Input::Bytes(Box::pin(i));
    let o = Output::Bytes(Box::pin(o));
    let gatekeeper = TunnelRelay::run(t, i, o, None, Some(sturdy::Oid(0.into())), output_text).unwrap();
    let main_entity = t.create(during::entity(initial_state).on_asserted(move |state, t, a: gatekeeper::Resolved| {
        match a {
            gatekeeper::Resolved::Accepted { responder_session } => f(state, t, responder_session),
            gatekeeper::Resolved::Rejected(r) => Err(error("Resolve rejected", r.detail))?,
        }
    }));
    let step = gatekeeper::Step::parse(&step.unparse())?;
    gatekeeper.assert(t, &gatekeeper::Resolve::<Arc<Cap>> {
        step,
        observer: Cap::guard(main_entity),
    });
    Ok(())
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

/// Main entry point for stdio-based Syndicate services.
pub async fn stdio_service<F>(f: F) -> !
where
    F: 'static + Send + FnOnce(&mut Activation) -> Result<Arc<Cap>, ActorError>
{
    let result = Actor::top(None, move |t| {
        let service = f(t)?;
        Ok(TunnelRelay::stdio_service(t, service))
    }).await;

    // Because we're currently using tokio::io::stdin(), which can prevent shutdown of the
    // runtime, this routine uses std::process::exit directly as a special case. It's a
    // stopgap: eventually, we'd like to do things Properly, as indicated in the comment
    // attached (at the time of writing) to tokio::io::stdin(), which reads in part:
    //
    //    This handle is best used for non-interactive uses, such as when a file
    //    is piped into the application. For technical reasons, `stdin` is
    //    implemented by using an ordinary blocking read on a separate thread, and
    //    it is impossible to cancel that read. This can make shutdown of the
    //    runtime hang until the user presses enter.
    //
    //    For interactive uses, it is recommended to spawn a thread dedicated to
    //    user input and use blocking IO directly in that thread.
    //
    // TODO: Revisit this.

    match result {
        Ok(Ok(())) => {
            std::process::exit(0);
        }
        Ok(Err(e)) => {
            tracing::error!("Main stdio_service actor failed: {}", e);
            std::process::exit(1);
        },
        Err(e) => {
            tracing::error!("Join of main stdio_service actor failed: {}", e);
            std::process::exit(2);
        }
    }
}

impl TunnelRelay {
    pub fn stdio_service(t: &mut Activation, service: Arc<Cap>) -> () {
        TunnelRelay::run(t,
                         Input::Bytes(Box::pin(tokio::io::stdin())),
                         Output::Bytes(Box::pin(tokio::io::stdout())),
                         Some(service),
                         None,
                         false);
    }

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
                      input_loop(t.trace_collector(), t.facet_ref(), i, tr_ref));
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
                reimported_attenuations: Map::new(),
            },
            pending_outbound: Vec::new(),
        };
        if let Some(ir) = initial_ref {
            tr.membranes.export_ref(ir).inc_ref();
        }
        let result = initial_oid.map(
            |io| Arc::clone(&tr.membranes.import_oid(t, &tr_ref, io.0).inc_ref().obj));
        dump_membranes!(tr.membranes);
        *tr_ref.lock() = Some(tr);
        t.add_exit_hook(&self_entity);
        (result, tr_ref, output_rx)
    }

    fn deserialize_one(&mut self, t: &mut Activation, bs: &[u8]) -> Result<Option<usize>, ActorError> {
        let mut src = BytesBinarySource::new(&bs);
        match src.peek() {
            Ok(Some(v)) => if v >= 128 {
                self.output_text = false;
                let mut r = src.into_packed();

                // Previously there was a "fast path" for Turn packets here, but keeping the
                // error handling consistent in cases of e.g. incomplete (but arriving later!)
                // packets was challenging, so I've removed it again for now because I prefer
                // the robustness to the small speed increase. We can come back to it later.

                // Try skipping a value without allocating first; only if this succeeds do we
                // actually parse. When transferring e.g. large ByteStrings this avoids repeated
                // large allocations that then go unused.
                //
                let mark = preserves::Reader::mark(&mut r)?;
                match r.skip_value() {
                    Err(e) if e.is_eof() => Ok(None),
                    Err(e) => Err(e)?,
                    Ok(()) => {
                        preserves::Reader::restore(&mut r, mark)?;
                        Ok(self.deserialize_one_from(t, &mut r)?.then(|| r.source.index as usize))
                    }
                }
            } else {
                self.output_text = true;
                let mut r = src.into_text();
                Ok(self.deserialize_one_from(t, &mut r)?.then(|| r.source.index as usize))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    fn deserialize_one_from<'de, R: Reader<'de>>(
        &mut self,
        t: &mut Activation,
        r: &mut R,
    ) -> Result<bool, ActorError> {
        let mut dec = ActivatedMembranes {
            turn: t,
            tr_ref: &self.self_ref,
            membranes: &mut self.membranes,
            rollback_symbols: vec![],
        };
        match P::Packet::deserialize(r, &mut dec) {
            Ok(res) => {
                self.handle_inbound_packet(t, res)?;
                Ok(true)
            }
            Err(e) => {
                dec.rollback();
                if e.is_eof() {
                    Ok(false)
                } else {
                    Err(e)?
                }
            }
        }
    }

    pub fn handle_inbound_datagram(&mut self, t: &mut Activation, bs: &[u8]) -> ActorResult {
        tracing::trace!(bytes = ?bs, "inbound datagram");
        self.deserialize_one(t, bs)?;
        Ok(())
    }

    pub fn handle_inbound_stream(&mut self, t: &mut Activation, buf: &mut BytesMut) -> ActorResult {
        loop {
            tracing::trace!(buffer = ?buf, "inbound stream");
            match self.deserialize_one(t, buf)? {
                Some(count) => buf.advance(count),
                None => return Ok(()),
            }
        }
    }

    fn _handle_inbound_event(&mut self, t: &mut Activation, oid: &SignedInteger, event: P::Event<Arc<Cap>>) -> ActorResult {
        tracing::trace!(?oid, ?event, "handle_inbound");
        let target = match self.membranes.exported.oid_map.get(oid) {
            Some(ws) =>
                ws.inc_ref(),
            None => {
                tracing::debug!(
                    event = ?(&P::TurnEvent { oid: P::Oid(oid.to_owned()), event }).unparse(),
                    "Cannot deliver event: nonexistent oid");
                return Ok(());
            }
        };
        let mut pins = vec![target.clone()];
        let target = Arc::clone(&target.obj);
        match event {
            P::Event::Assert(b) => {
                let P::Assert { assertion: P::Assertion(a), handle: remote_handle } = b;
                a.foreach_embedded(
                    &mut |r| Ok(pins.push(self.membranes.lookup_ref(r)))).unwrap();
                if let Some(local_handle) = target.assert(t, &a) {
                    if let Some(_) = self.inbound_assertions.insert(remote_handle, (local_handle, pins)) {
                        return Err(error("Assertion with duplicate handle", AnyValue::new(false)))?;
                    }
                } else {
                    self.membranes.release(pins);
                }
                dump_membranes!(self.membranes);
            }
            P::Event::Retract(b) => {
                let P::Retract { handle: remote_handle } = b;
                match self.inbound_assertions.remove(&remote_handle) {
                    None => {
                        // This can happen when e.g. an assertion previously made
                        // failed to pass an attenuation filter
                        tracing::debug!(?remote_handle, "Retraction of nonexistent handle");
                    }
                    Some((local_handle, previous_pins)) => {
                        self.membranes.release(previous_pins);
                        self.membranes.release(pins);
                        t.retract(local_handle);
                        dump_membranes!(self.membranes);
                    }
                }
            }
            P::Event::Message(b) => {
                let P::Message { body: P::Assertion(a) } = b;
                a.foreach_embedded(&mut |r| {
                    let ws = self.membranes.lookup_ref(r);
                    let rc = ws.current_ref_count();
                    pins.push(ws);
                    match rc {
                        1 => Err(()),
                        _ => Ok(()),
                    }
                }).map_err(|_| error("Cannot receive transient reference", AnyValue::new(false)))?;
                target.message(t, &a);
                self.membranes.release(pins);
                dump_membranes!(self.membranes);
            }
            P::Event::Sync(b) => {
                let P::Sync { peer } = b;
                pins.push(self.membranes.lookup_ref(&peer));
                dump_membranes!(self.membranes);
                struct SyncPeer {
                    relay_ref: TunnelRelayRef,
                    peer: Arc<Cap>,
                    pins: Vec<Arc<WireSymbol>>,
                }
                impl Entity<Synced> for SyncPeer {
                    fn message(&mut self, t: &mut Activation, _a: Synced) -> ActorResult {
                        self.peer.message(t, &AnyValue::new(true));
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
        Ok(())
    }

    pub fn handle_inbound_packet(&mut self, t: &mut Activation, p: P::Packet<Arc<Cap>>) -> ActorResult {
        tracing::debug!(packet = ?p, "-->");
        match p {
            P::Packet::Extension(P::Extension { label, fields }) => {
                tracing::info!(?label, ?fields, "received Extension from peer");
                Ok(())
            }
            P::Packet::Nop(_b) => {
                tracing::trace!("received Nop from peer");
                Ok(())
            }
            P::Packet::Error(b) => {
                tracing::info!(message = ?b.message.clone(),
                               detail = ?b.detail.clone(),
                               "received Error from peer");
                Err(b)?
            }
            P::Packet::Turn(P::Turn(events)) => {
                for P::TurnEvent { oid, event } in events {
                    self._handle_inbound_event(t, &oid.0, event)?;
                }
                t.commit()
            }
        }
    }

    fn outbound_event_bookkeeping(
        &mut self,
        _t: &mut Activation,
        remote_oid: SignedInteger,
        event: &P::Event<Arc<Cap>>,
    ) -> ActorResult {
        match event {
            P::Event::Assert(b) => {
                let P::Assert { assertion: P::Assertion(a), handle } = b;
                if let Some(target_ws) = self.membranes.imported.oid_map.get(&remote_oid).map(Arc::clone) {
                    target_ws.inc_ref(); // encoding won't do this; target oid is syntactically special
                    let mut pins = vec![target_ws];
                    a.foreach_embedded(
                        &mut |r| Ok(pins.push(self.membranes.lookup_ref(r)))).unwrap();
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
                let P::Retract { handle } = b;
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
                let P::Message { body: P::Assertion(a) } = b;
                a.foreach_embedded(&mut |r| {
                    let ws = self.membranes.lookup_ref(r);
                    if self.membranes.release_one(ws) { // undo the inc_ref from encoding
                        Err(())
                    } else {
                        Ok(())
                    }
                }).map_err(|_| error("Sent transient reference", AnyValue::new(false)))?;
                dump_membranes!(self.membranes);
            },
            P::Event::Sync(_b) =>
                todo!(),
        }
        Ok(())
    }

    pub fn send_packet(&mut self, account: &Arc<Account>, cost: usize, p: &P::Packet<Arc<Cap>>) -> ActorResult {
        let item = p.unparse();
        tracing::debug!(packet = ?item, "<--");

        let bs = if self.output_text {
            let mut s = TextWriter::encode(&mut self.membranes, &item)?;
            s.push('\n');
            s.into_bytes()
        } else {
            PackedWriter::encode(&mut self.membranes, &item)?
        };
        tracing::trace!(buffer = ?bs, "outbound bytes");

        let _ = self.output.send(LoanedItem::new(account, cost, bs));
        Ok(())
    }

    pub fn send_event(&mut self, t: &mut Activation, remote_oid: SignedInteger, event: P::Event<Arc<Cap>>) -> ActorResult {
        if self.pending_outbound.is_empty() {
            let self_ref = Arc::clone(&self.self_ref);
            t.pre_commit(move |t| {
                let mut g = self_ref.lock();
                let tr = g.as_mut().expect("initialized");

                let events = std::mem::take(&mut tr.pending_outbound);
                // Avoid a .clone() of events by putting it into a packet and then *getting it back out again*
                let count = events.len();
                let packet = P::Packet::Turn(P::Turn(events));
                tr.send_packet(&t.account(), count, &packet)?;
                let events = match packet { P::Packet::Turn(P::Turn(e)) => e, _ => unreachable!() };

                for P::TurnEvent { oid, event } in events.into_iter() {
                    tr.outbound_event_bookkeeping(t, oid.0, &event)?;
                }
                Ok(())
            });
        }
        self.pending_outbound.push(P::TurnEvent { oid: P::Oid(remote_oid), event });
        Ok(())
    }
}

impl Membranes {
    fn export_ref(&mut self, obj: Arc<Cap>) -> Arc<WireSymbol> {
        let oid = SignedInteger::from(self.next_export_oid as u128);
        self.next_export_oid += 1;
        self.exported.insert(oid, obj)
    }

    fn import_oid(
        &mut self,
        t: &mut Activation,
        relay_ref: &TunnelRelayRef,
        oid: SignedInteger,
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
            if let WireSymbolSide::Exported = ws.side {
                self.reimported_attenuations.remove(&ws.oid);
            }
            self.membrane(ws.side).remove(&ws);
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

    fn decode_wireref(
        &mut self,
        t: &mut Activation,
        relay_ref: &TunnelRelayRef,
        wr: sturdy::WireRef,
    ) -> ReaderResult<(Arc<WireSymbol>, Arc<Cap>)> {
        let (ws, cap) = match wr {
            sturdy::WireRef::Mine{ oid } => {
                let ws = self.imported.oid_map.get(&oid.0).map(Arc::clone)
                    .unwrap_or_else(|| self.import_oid(t, relay_ref, oid.0));
                let obj = Arc::clone(&ws.obj);
                (ws, obj)
            }
            sturdy::WireRef::Yours { oid, attenuation } => {
                let ws = self.exported.oid_map.get(&oid.0).map(Arc::clone)
                    .unwrap_or_else(|| self.exported.insert_inert_entity(t, oid.0.clone()));

                if attenuation.is_empty() {
                    let obj = Arc::clone(&ws.obj);
                    (ws, obj)
                } else {
                    let attenuated_obj = ws.obj.attenuate(&attenuation)
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid capability attenuation: {:?}", e))
                        })?;

                    let variations = self.reimported_attenuations.entry(oid.0).or_default();
                    match variations.get(&attenuated_obj) {
                        None => {
                            variations.insert(Arc::clone(&attenuated_obj));
                            self.exported.ref_map.insert(Arc::clone(&attenuated_obj), Arc::clone(&ws));
                            (ws, attenuated_obj)
                        }
                        Some(existing) =>
                            (ws, Arc::clone(existing))
                    }
                }
            }
        };
        ws.inc_ref();
        Ok((ws, cap))
    }

    fn encode_wireref(&mut self, d: &Arc<Cap>) -> sturdy::WireRef {
        match self.exported.ref_map.get(d) {
            Some(ws) => sturdy::WireRef::Mine {
                oid: sturdy::Oid(ws.inc_ref().oid.clone()),
            },
            None => match self.imported.ref_map.get(d) {
                Some(ws) => {
                    if d.attenuation.is_empty() {
                        sturdy::WireRef::Yours {
                            oid: sturdy::Oid(ws.inc_ref().oid.clone()),
                            attenuation: vec![],
                        }
                    } else {
                        // We may trust the peer to enforce attenuation on our behalf, in
                        // which case we can return sturdy::WireRef::Yours with an attenuation
                        // attached here, but for now we don't.
                        sturdy::WireRef::Mine {
                            oid: sturdy::Oid(self.export_ref(Arc::clone(d)).inc_ref().oid.clone()),
                        }
                    }
                }
                None =>
                    sturdy::WireRef::Mine {
                        oid: sturdy::Oid(self.export_ref(Arc::clone(d)).inc_ref().oid.clone()),
                    },
            }
        }
    }
}

impl<'a, 'm> ActivatedMembranes<'a, 'm> {
    fn store_rollback(&mut self, ws: Arc<WireSymbol>, obj: Arc<Cap>) -> ReaderResult<Arc<Cap>> {
        self.rollback_symbols.push(ws);
        Ok(obj)
    }

    fn rollback(&mut self) {
        let symbols = std::mem::replace(&mut self.rollback_symbols, vec![]);
        for ws in symbols.into_iter() {
            ws.dec_ref();
        }
    }
}

impl<'a, 'm> DomainDecode<Arc<Cap>> for ActivatedMembranes<'a, 'm> {
    fn decode_embedded<'de, R: Reader<'de> + ?Sized, VR: IOValueReader + ?Sized>(
        &mut self,
        r: &mut R,
        _read_annotations: bool,
    ) -> ReaderResult<Arc<Cap>> {
        let wr = sturdy::WireRef::deserialize(r, self)?;
        let (ws, obj) = self.membranes.decode_wireref(self.turn, self.tr_ref, wr)?;
        self.store_rollback(ws, obj)
    }

    fn decode_value(&mut self, v: IOValue) -> ReaderResult<Arc<Cap>> {
        let v = value_map_embedded(&v, &mut |c| self.decode_value(c.clone()))?;
        let wr = sturdy::WireRef::parse(&v)?;
        let (ws, obj) = self.membranes.decode_wireref(self.turn, self.tr_ref, wr)?;
        self.store_rollback(ws, obj)
    }
}

impl DomainEncode<Arc<Cap>> for Membranes {
    fn encode_embedded(
        &mut self,
        w: &mut dyn preserves::Writer,
        d: &Arc<Cap>,
    ) -> io::Result<()> {
        self.encode_wireref(d).unparse().write(w, self)
    }

    fn encode_value(&mut self, d: &Arc<Cap>) -> io::Result<IOValue> {
        let v = self.encode_wireref(d).unparse();
        let v = value_map_embedded(&v, &mut |c| self.encode_value(c))?;
        Ok(v.into())
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
                    Err(e) => {
                        if e.kind() == io::ErrorKind::ConnectionReset {
                            break;
                        }
                        return Err(e)?;
                    }
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
        if let ExitStatus::Error(e) = &**exit_status {
            let e = e.clone();
            let mut g = self.relay_ref.lock();
            let tr = g.as_mut().expect("initialized");
            if let Err(f) = tr.send_packet(&t.account(), 1, &P::Packet::Error(e)) {
                tracing::error!("Failed to send error packet: {:?}", f);
            }
        }
    }
}

impl Entity<AnyValue> for RelayEntity {
    fn assert(&mut self, t: &mut Activation, a: AnyValue, h: Handle) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Assert(P::Assert {
                assertion: P::Assertion(a),
                handle: P::Handle(h.into()),
            }))
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Retract(P::Retract {
                handle: P::Handle(h.into()),
            }))
    }
    fn message(&mut self, t: &mut Activation, m: AnyValue) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Message(P::Message {
                body: P::Assertion(m)
            }))
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        self.relay_ref.lock().as_mut().expect("initialized")
            .send_event(t, self.oid.clone(), P::Event::Sync(P::Sync {
                peer: Cap::guard(peer)
            }))
    }
}
