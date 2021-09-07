use bytes::Buf;
use bytes::BytesMut;

use crate::actor::*;
use crate::during;
use crate::error::Error;
use crate::error::error;
use crate::schemas::gatekeeper;
use crate::schemas::external_protocol as P;
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
use preserves::value::PackedWriter;
use preserves::value::TextWriter;
use preserves::value::Value;
use preserves::value::ViaCodec;
use preserves::value::Writer;
use preserves::value::signed_integer::SignedInteger;

use preserves_schema::support::Deserialize;
use preserves_schema::support::ParseError;

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

struct WireRefCodec;

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

type Pins<'a> = &'a mut Vec<Arc<WireSymbol>>;

//---------------------------------------------------------------------------

impl WireSymbol {
    #[inline]
    fn inc_ref<'a>(self: &'a Arc<Self>, pins: Pins) -> &'a Arc<Self> {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        pins.push(Arc::clone(&self));
        tracing::trace!(?self, "acquire");
        self
    }

    #[inline]
    fn dec_ref(&self) -> bool {
        let old_count = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        tracing::trace!(?self, "release");
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
}

impl std::fmt::Debug for Membrane {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.debug_struct("Membrane")
            .field("side", &self.side)
            .field("refs", &self.oid_map.values())
            .finish()
    }
}

impl Membranes {
    fn export_ref(&mut self, obj: Arc<Cap>) -> Arc<WireSymbol> {
        let ws = match self.exported.ref_map.get(&obj) {
            None => {
                let oid = sturdy::Oid(SignedInteger::from(self.next_export_oid as u128));
                self.next_export_oid += 1;
                self.exported.insert(oid, obj)
            }
            Some(ws) => Arc::clone(ws)
        };
        ws
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

    fn membrane(&mut self, side: WireSymbolSide) -> &mut Membrane {
        match side {
            WireSymbolSide::Imported => &mut self.imported,
            WireSymbolSide::Exported => &mut self.exported,
        }
    }

    #[inline]
    fn release<I: IntoIterator<Item = Arc<WireSymbol>>>(&mut self, wss: I) {
        for ws in wss {
            if ws.dec_ref() {
                let membrane = self.membrane(ws.side);
                membrane.oid_map.remove(&ws.oid);
                membrane.ref_map.remove(&ws.obj);
            }
        }
    }
}

impl DomainEncode<P::_Ptr> for WireRefCodec {
    fn encode_embedded<W: Writer>(
        &mut self,
        w: &mut W,
        d: &P::_Ptr,
    ) -> io::Result<()> {
        w.write(&mut NoEmbeddedDomainCodec, &IOValue::from(&**d))
    }
}

impl DomainDecode<P::_Ptr> for WireRefCodec {
    fn decode_embedded<'de, 'src, S: BinarySource<'de>>(
        &mut self,
        src: &'src mut S,
        _read_annotations: bool,
    ) -> io::Result<P::_Ptr> {
        Ok(Arc::new(P::_Dom::deserialize(&mut src.packed(NoEmbeddedDomainCodec))?))
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
    let main_entity = t.create(during::entity(initial_state).on_asserted(move |state, t, a: AnyValue| {
        let denotation = a.value().to_embedded()?;
        f(state, t, Arc::clone(denotation))
    }));
    gatekeeper.assert(t, &gatekeeper::Resolve {
        sturdyref,
        observer: Cap::new(&main_entity),
    });
}

// macro_rules! dump_membranes { ($e:expr) => { tracing::trace!("membranes: {:#?}", $e); } }
macro_rules! dump_membranes { ($e:expr) => { (); } }

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
        let self_entity = t.create(TunnelRefEntity {
            relay_ref: Arc::clone(&tr_ref),
        });
        let mut tr = TunnelRelay {
            self_ref: Arc::clone(&tr_ref),
            output: output_tx,
            output_text: false,
            inbound_assertions: Map::new(),
            outbound_assertions: Map::new(),
            membranes: Membranes {
                exported: Membrane::new(WireSymbolSide::Exported),
                imported: Membrane::new(WireSymbolSide::Imported),
                next_export_oid: 0,
            },
            pending_outbound: Vec::new(),
            self_entity: self_entity.clone(),
        };
        if let Some(ir) = initial_ref {
            tr.membranes.export_ref(ir).inc_ref(&mut vec![]);
        }
        let result = initial_oid.map(
            |io| Arc::clone(&tr.membranes.import_oid(t, &tr_ref, io).obj));
        dump_membranes!(tr.membranes);
        *tr_ref.lock().unwrap() = Some(tr);
        t.linked_task(crate::name!("writer"), output_loop(o, output_rx));
        t.linked_task(crate::name!("reader"), input_loop(t.facet.clone(), i, tr_ref));
        t.state.add_exit_hook(&self_entity);
        result
    }

    fn deserialize_one(&mut self, bs: &[u8]) -> (Result<P::Packet, ParseError>, usize) {
        let mut src = BytesBinarySource::new(&bs);
        match src.peek() {
            Ok(v) => if v >= 128 {
                self.output_text = false;
                let mut r = src.packed(WireRefCodec);
                let res = P::Packet::deserialize(&mut r);
                (res, r.source.index)
            } else {
                self.output_text = true;
                let mut dec = ViaCodec::new(WireRefCodec);
                let mut r = src.text::<_, P::_Any, _>(&mut dec);
                let res = P::Packet::deserialize(&mut r);
                (res, r.source.index)
            },
            Err(e) => (Err(e.into()), 0)
        }
    }

    fn handle_inbound_datagram(&mut self, t: &mut Activation, bs: &[u8]) -> ActorResult {
        let item = self.deserialize_one(bs).0?;
        self.handle_inbound_packet(t, item)
    }

    fn handle_inbound_stream(&mut self, t: &mut Activation, buf: &mut BytesMut) -> ActorResult {
        loop {
            let (result, count) = self.deserialize_one(buf);
            match result {
                Err(ParseError::Preserves(PreservesError::Io(e)))
                    if is_eof_io_error(&e) => return Ok(()),
                Err(e) =>
                    return Err(e)?,
                Ok(item) => {
                    buf.advance(count);
                    self.handle_inbound_packet(t, item)?;
                }
            }
        }
    }

    fn import_wire_ref(&mut self, t: &mut Activation, d: &Arc<sturdy::WireRef>, pins: Pins) -> io::Result<Arc<Cap>> {
        match &**d {
            sturdy::WireRef::Mine { oid: b } => {
                let oid = &**b;
                let ws = match self.membranes.imported.oid_map.get(&oid) {
                    Some(ws) => Arc::clone(ws),
                    None => self.membranes.import_oid(t, &self.self_ref, oid.clone()),
                };
                Ok(Arc::clone(&ws.inc_ref(pins).obj))
            }
            sturdy::WireRef::Yours { oid: b, attenuation } => {
                let oid = &**b;
                match self.membranes.exported.oid_map.get(&oid) {
                    Some(ws) => {
                        ws.inc_ref(pins);
                        if attenuation.is_empty() {
                            Ok(Arc::clone(&ws.obj))
                        } else {
                            ws.obj.attenuate(&sturdy::Attenuation(attenuation.clone()))
                                .map_err(|e| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("Invalid capability attenuation: {:?}", e))
                                })
                        }
                    }
                    None => Ok(Cap::new(&t.inert_entity())),
                }
            }
        }
    }

    #[inline]
    fn import<V: Into<P::_Any>>(&mut self, t: &mut Activation, v: V, pins: Pins) -> io::Result<AnyValue> {
        v.into().copy_via(&mut |d| Ok(Value::Embedded(self.import_wire_ref(t, d, pins)?)))
    }

    #[inline]
    fn wire_symbol_for_imported_oid(&mut self, oid: &sturdy::Oid) -> &Arc<WireSymbol> {
        self.membranes.imported.oid_map.get(oid).expect("imported oid entry to exist for RelayEntity")
    }

    fn export_cap(&mut self, d: &Arc<Cap>, pins: Pins) -> io::Result<Arc<sturdy::WireRef>> {
        Ok(Arc::new(match self.membranes.exported.ref_map.get(d) {
            Some(ws) => sturdy::WireRef::Mine {
                oid: Box::new(ws.inc_ref(pins).oid.clone()),
            },
            None => match self.membranes.imported.ref_map.get(d) {
                Some(ws) => {
                    if d.attenuation.is_empty() {
                        sturdy::WireRef::Yours {
                            oid: Box::new(ws.inc_ref(pins).oid.clone()),
                            attenuation: vec![],
                        }
                    } else {
                        // We may trust the peer to enforce attenuation on our behalf, in
                        // which case we can return sturdy::WireRef::Yours with an attenuation
                        // attached here, but for now we don't.
                        sturdy::WireRef::Mine {
                            oid: Box::new(self.membranes.export_ref(Arc::clone(d)).inc_ref(pins).oid.clone()),
                        }
                    }
                }
                None =>
                    sturdy::WireRef::Mine {
                        oid: Box::new(self.membranes.export_ref(Arc::clone(d)).inc_ref(pins).oid.clone()),
                    },
            }
        }))
    }

    #[inline]
    fn export<V: Into<AnyValue>>(&mut self, v: V, pins: Pins) -> io::Result<P::_Any> {
        v.into().copy_via(&mut |d| Ok(Value::Embedded(self.export_cap(d, pins)?)))
    }

    fn handle_inbound_packet(&mut self, t: &mut Activation, p: P::Packet) -> ActorResult {
        tracing::trace!(packet = ?p, "-->");
        match p {
            P::Packet::Error(b) => {
                tracing::info!(message = ?b.message.clone(),
                               detail = ?b.detail.clone(),
                               "received Error from peer");
                let P::Error { message, detail } = *b;
                Err(error(&message, self.import(t, detail, &mut vec![])?))
            },
            P::Packet::Turn(b) => {
                let P::Turn(events) = *b;
                for P::TurnEvent { oid, event } in events {
                    let target = match self.membranes.exported.oid_map.get(&sturdy::Oid(oid.0.clone())) {
                        Some(ws) => Arc::clone(ws),
                        None => return Err(
                            error("Cannot deliver event: nonexistent oid",
                                  self.import(t, &P::TurnEvent { oid, event }, &mut vec![])?)),
                    };
                    match event {
                        P::Event::Assert(b) => {
                            let P::Assert { assertion: P::Assertion(a), handle: remote_handle } = *b;
                            let mut pins = vec![];
                            target.inc_ref(&mut pins);
                            let a = self.import(t, a, &mut pins)?;
                            dump_membranes!(self.membranes);
                            if let Some(local_handle) = target.obj.assert(t, a) {
                                if let Some(_) = self.inbound_assertions.insert(remote_handle, (local_handle, pins)) {
                                    return Err(error("Assertion with duplicate handle",
                                                     AnyValue::new(false)));
                                }
                            }
                        }
                        P::Event::Retract(b) => {
                            let P::Retract { handle: remote_handle } = *b;
                            let (local_handle, pins) = match self.inbound_assertions.remove(&remote_handle) {
                                None => return Err(error("Retraction of nonexistent handle",
                                                         self.import(t, &remote_handle, &mut vec![])?)),
                                Some(wss) => wss,
                            };
                            self.membranes.release(pins);
                            dump_membranes!(self.membranes);
                            t.retract(local_handle);
                        }
                        P::Event::Message(b) => {
                            let P::Message { body: P::Assertion(a) } = *b;
                            let mut pins = vec![];
                            let a = self.import(t, a, &mut pins)?;
                            ensure_no_transient_references(&pins)?;
                            target.obj.message(t, a);
                            self.membranes.release(pins);
                            dump_membranes!(self.membranes);
                        }
                        P::Event::Sync(b) => {
                            let P::Sync { peer } = *b;
                            let mut pins = vec![];
                            target.inc_ref(&mut pins);
                            let peer = self.import_wire_ref(t, &peer, &mut pins)?;
                            dump_membranes!(self.membranes);
                            struct SyncPeer {
                                relay_ref: TunnelRelayRef,
                                peer: Arc<Cap>,
                                pins: Vec<Arc<WireSymbol>>,
                            }
                            impl Entity<Synced> for SyncPeer {
                                fn message(&mut self, t: &mut Activation, _a: Synced) -> ActorResult {
                                    self.peer.message(t, AnyValue::new(true));
                                    let mut g = self.relay_ref.lock().expect("unpoisoned");
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
                            t.sync(&peer.underlying, k);
                        }
                    }
                }
                t.deliver();
                Ok(())
            }
        }
    }

    fn encode_packet(&mut self, p: P::Packet) -> Result<Vec<u8>, Error> {
        let item = P::_Any::from(&p);
        tracing::trace!(packet = ?item, "<--");
        if self.output_text {
            let mut s = TextWriter::encode(&mut WireRefCodec, &item)?;
            s.push('\n');
            Ok(s.into_bytes())
        } else {
            Ok(PackedWriter::encode(&mut WireRefCodec, &item)?)
        }
    }

    pub fn send_packet(&mut self, account: &Arc<Account>, cost: usize, p: P::Packet) -> ActorResult {
        let bs = self.encode_packet(p)?;
        let _ = self.output.send(LoanedItem::new(account, cost, bs));
        Ok(())
    }

    pub fn send_event(&mut self, t: &mut Activation, oid: sturdy::Oid, event: P::Event) -> ActorResult {
        if self.pending_outbound.is_empty() {
            t.message_for_myself(&self.self_entity, ());
        }
        let turn_event = P::TurnEvent {
            oid: P::Oid(oid.0),
            event,
        };
        self.pending_outbound.push(turn_event);
        Ok(())
    }
}

async fn input_loop(
    facet: FacetRef,
    i: Input,
    relay: TunnelRelayRef,
) -> ActorResult {
    let account = Account::new(crate::name!("input-loop"));
    match i {
        Input::Packets(mut src) => {
            loop {
                account.ensure_clear_funds().await;
                match src.next().await {
                    None => return facet.activate(Arc::clone(&account), |t| Ok(t.state.shutdown())),
                    Some(bs) => facet.activate(Arc::clone(&account), |t| {
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
                account.ensure_clear_funds().await;
                buf.reserve(BUFSIZE);
                let n = match r.read_buf(&mut buf).await {
                    Ok(n) => n,
                    Err(e) =>
                        if e.kind() == io::ErrorKind::ConnectionReset {
                            return facet.activate(Arc::clone(&account), |t| Ok(t.state.shutdown()));
                        } else {
                            return Err(e)?;
                        },
                };
                match n {
                    0 => return facet.activate(Arc::clone(&account), |t| Ok(t.state.shutdown())),
                    _ => facet.activate(Arc::clone(&account), |t| {
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
        tr.send_packet(&t.account(), events.len(), P::Packet::Turn(Box::new(P::Turn(events))))
    }

    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        if let Err(e) = &**exit_status {
            let mut g = self.relay_ref.lock().expect("unpoisoned");
            let tr = g.as_mut().expect("initialized");
            let crate::schemas::internal_protocol::Error { message, detail } = e;
            let e = P::Error {
                message: message.clone(),
                detail: tr.export(detail.clone(), &mut vec![])?,
            };
            tr.send_packet(&t.account(), 1, P::Packet::Error(Box::new(e)))?;
        }
        Ok(())
    }
}

#[inline]
fn ensure_no_transient_references(pins: &Vec<Arc<WireSymbol>>) -> ActorResult {
    for ws in pins.iter() {
        if ws.current_ref_count() == 1 {
            return Err(error("Cannot receive transient reference", AnyValue::new(false)));
        }
    }
    Ok(())
}

impl RelayEntity {
    fn with_tunnel_relay<F: FnOnce(&mut TunnelRelay) -> ActorResult>(
        &mut self,
        f: F,
    ) -> ActorResult {
        let mut g = self.relay_ref.lock().expect("unpoisoned");
        let tr = g.as_mut().expect("initialized");
        f(tr)
    }
}

impl Entity<AnyValue> for RelayEntity {
    fn assert(&mut self, t: &mut Activation, a: AnyValue, h: Handle) -> ActorResult {
        let oid = self.oid.clone();
        self.with_tunnel_relay(|tr| {
            let handle = P::Handle(h.into());

            let mut pins = vec![];
            tr.wire_symbol_for_imported_oid(&oid).inc_ref(&mut pins);
            let a = tr.export(a, &mut pins)?;
            tr.outbound_assertions.insert(handle.clone(), pins);
            dump_membranes!(tr.membranes);

            tr.send_event(t, oid, P::Event::Assert(Box::new(P::Assert {
                assertion: P::Assertion(a),
                handle,
            })))
        })
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        let oid = self.oid.clone();
        self.with_tunnel_relay(|tr| {
            let handle = P::Handle(h.into());

            if let Some(outbound) = tr.outbound_assertions.remove(&handle) {
                tr.membranes.release(outbound);
            }
            dump_membranes!(tr.membranes);

            tr.send_event(t, oid, P::Event::Retract(Box::new(P::Retract {
                handle,
            })))
        })
    }
    fn message(&mut self, t: &mut Activation, m: AnyValue) -> ActorResult {
        let oid = self.oid.clone();
        self.with_tunnel_relay(|tr| {
            let mut pins = vec![];
            let m = tr.export(m, &mut pins)?;
            ensure_no_transient_references(&pins)?;

            tr.send_event(t, oid, P::Event::Message(Box::new(P::Message {
                body: P::Assertion(m)
            })))?;
            tr.membranes.release(pins);
            dump_membranes!(tr.membranes);
            Ok(())
        })
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        todo!("TODO not yet implemented");

        // let oid = self.oid.clone();
        // self.with_tunnel_relay(|tr| {
        //     ...
        //     tr.wire_symbol_for_imported_oid(&oid).inc_ref(&mut pins); etc. etc.
        //
        //     tr.send_event(t, oid, P::Event::Sync(Box::new(P::Sync {
        //         peer: Cap::guard(&peer)
        //     })))
        //     dump_membranes!(tr.membranes); etc. etc.
        // })
    }
}
