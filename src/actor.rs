pub use futures::future::BoxFuture;

pub use std::future::ready;

use super::ActorId;
use super::schemas::internal_protocol::*;
use super::schemas::sturdy;
use super::error::Error;
use super::error::error;
use super::rewrite::CaveatError;
use super::rewrite::CheckedCaveat;

use preserves::value::Domain;
use preserves::value::IOValue;
use preserves::value::Map;
use preserves::value::NestedValue;

use std::boxed::Box;
use std::collections::hash_map::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

use tracing;
use tracing::Instrument;

pub use super::schemas::internal_protocol::_Any;
pub use super::schemas::internal_protocol::Handle;
pub use super::schemas::internal_protocol::Oid;

pub type ActorResult = Result<(), Error>;
pub type ActorHandle = tokio::task::JoinHandle<ActorResult>;

pub trait Entity: Send {
    fn assert(&mut self, _t: &mut Activation, _a: _Any, _h: Handle) -> ActorResult {
        Ok(())
    }
    fn retract(&mut self, _t: &mut Activation, _h: Handle) -> ActorResult {
        Ok(())
    }
    fn message(&mut self, _t: &mut Activation, _m: _Any) -> ActorResult {
        Ok(())
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref>) -> ActorResult {
        t.message(&peer, _Any::new(true));
        Ok(())
    }
    fn turn_end(&mut self, _t: &mut Activation) -> ActorResult {
        Ok(())
    }
    fn exit_hook(&mut self, _t: &mut Activation, _exit_status: &ActorResult) -> BoxFuture<ActorResult> {
        Box::pin(ready(Ok(())))
    }
}

enum Destination {
    ImmediateSelf(Oid),
    Remote(Arc<Ref>),
}

type OutboundAssertions = Map<Handle, Destination>;
type PendingEventQueue = Vec<(Arc<Ref>, Event)>;

// This is what other implementations call a "Turn", renamed here to
// avoid conflicts with schemas::internal_protocol::Turn.
pub struct Activation<'activation> {
    pub actor: &'activation mut Actor,
    queues: HashMap<ActorId, PendingEventQueue>,
    immediate_self: Vec<TurnEvent>,
}

#[derive(Debug)]
enum SystemMessage {
    Release,
    ReleaseOid(Oid),
    Turn(Turn),
    Crash(Error),
}

pub struct Mailbox {
    pub actor_id: ActorId,
    pub mailbox_id: u64,
    tx: UnboundedSender<SystemMessage>,
    queue_depth: Arc<AtomicUsize>,
    mailbox_count: Arc<AtomicUsize>,
}

pub struct Actor {
    actor_id: ActorId,
    tx: UnboundedSender<SystemMessage>,
    rx: Option<UnboundedReceiver<SystemMessage>>,
    queue_depth: Arc<AtomicUsize>,
    mailbox_count: Arc<AtomicUsize>,
    outbound_assertions: OutboundAssertions,
    oid_map: Map<Oid, Box<dyn Entity + Send>>,
    next_task_id: u64,
    linked_tasks: Map<u64, CancellationToken>,
    exit_hooks: Vec<Arc<Ref>>,
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectAddress {
    pub mailbox: Mailbox,
    pub oid: Oid,
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ref {
    pub addr: Arc<ObjectAddress>,
    pub attenuation: Vec<CheckedCaveat>,
}

//---------------------------------------------------------------------------

preserves_schema::support::lazy_static! {
    pub static ref INERT_REF: Arc<Ref> = {
        struct InertEntity;
        impl crate::actor::Entity for InertEntity {}
        let mut ac = Actor::new();
        let e = ac.create(InertEntity);
        ac.boot(tracing::info_span!(parent: None, "INERT_REF"),
                |t| Box::pin(ready(Ok(t.actor.shutdown()))));
        e
    };
}

impl<'activation> Activation<'activation> {
    pub fn for_actor(actor: &'activation mut Actor) -> Self {
        Activation {
            actor,
            queues: HashMap::new(),
            immediate_self: Vec::new(),
        }
    }

    fn immediate_oid(&self, r: &Arc<Ref>) -> Oid {
        if r.addr.mailbox.actor_id != self.actor.actor_id {
            panic!("Cannot use immediate_self to send to remote peers");
        }
        r.addr.oid.clone()
    }

    pub fn assert<M>(&mut self, r: &Arc<Ref>, a: M) -> Handle where M: Into<_Any> {
        let handle = crate::next_handle();
        if let Some(assertion) = r.rewrite(a.into()) {
            self.queue_for(r).push((Arc::clone(r), Event::Assert(Box::new(
                Assert { assertion, handle: handle.clone() }))));
            self.actor.outbound_assertions.insert(handle.clone(), Destination::Remote(Arc::clone(r)));
        }
        handle
    }

    pub fn assert_immediate_self<M>(&mut self, r: &Arc<Ref>, a: M) -> Handle where M: Into<_Any> {
        let oid = self.immediate_oid(r);
        let handle = crate::next_handle();
        if let Some(assertion) = r.rewrite(a.into()) {
            self.immediate_self.push(TurnEvent {
                oid: oid.clone(),
                event: Event::Assert(Box::new(
                    Assert { assertion, handle: handle.clone() })),
            });
            self.actor.outbound_assertions.insert(handle.clone(), Destination::ImmediateSelf(oid));
        }
        handle
    }

    pub fn retract(&mut self, handle: Handle) {
        if let Some(d) = self.actor.outbound_assertions.remove(&handle) {
            self.retract_known_ref(d, handle)
        }
    }

    fn retract_known_ref(&mut self, d: Destination, handle: Handle) {
        match d {
            Destination::Remote(r) =>
                self.queue_for(&r).push((r, Event::Retract(Box::new(Retract { handle })))),
            Destination::ImmediateSelf(oid) =>
                self.immediate_self.push(TurnEvent {
                    oid,
                    event: Event::Retract(Box::new(Retract { handle })),
                }),
        }
    }

    pub fn message<M>(&mut self, r: &Arc<Ref>, m: M) where M: Into<_Any> {
        if let Some(body) = r.rewrite(m.into()) {
            self.queue_for(r).push((Arc::clone(r), Event::Message(Box::new(
                Message { body }))))
        }
    }

    pub fn message_immediate_self<M>(&mut self, r: &Arc<Ref>, m: M) where M: Into<_Any> {
        if let Some(body) = r.rewrite(m.into()) {
            self.immediate_self.push(TurnEvent {
                oid: self.immediate_oid(r),
                event: Event::Message(Box::new(Message { body })),
            })
        }
    }

    pub fn sync(&mut self, r: &Arc<Ref>, peer: Arc<Ref>) {
        self.queue_for(r).push((Arc::clone(r), Event::Sync(Box::new(Sync { peer }))));
    }

    fn queue_for(&mut self, r: &Arc<Ref>) -> &mut PendingEventQueue {
        self.queues.entry(r.addr.mailbox.actor_id).or_default()
    }

    fn deliver(&mut self) {
        if !self.immediate_self.is_empty() {
            panic!("Unprocessed immediate_self events remain at deliver() time");
        }
        for (_actor_id, turn) in std::mem::take(&mut self.queues).into_iter() {
            if turn.len() == 0 { continue; }
            let first_ref = Arc::clone(&turn[0].0);
            let target = &first_ref.addr.mailbox;
            target.send(Turn(turn.into_iter().map(
                |(r, e)| TurnEvent { oid: r.addr.oid.clone(), event: e }).collect()));
        }
    }

    fn with_oid<R,
                    Ff: FnOnce(&mut Self) -> R,
                    Fs: FnOnce(&mut Self, &mut Box<dyn Entity + Send>) -> R>(
        &mut self,
        oid: &Oid,
        kf: Ff,
        ks: Fs,
    ) -> R {
        match self.actor.oid_map.remove_entry(&oid) {
            None => kf(self),
            Some((k, mut e)) => {
                let result = ks(self, &mut e);
                self.actor.oid_map.insert(k, e);
                result
            }
        }
    }
}

impl<'activation> Drop for Activation<'activation> {
    fn drop(&mut self) {
        self.deliver()
    }
}

impl Mailbox {
    pub fn send(&self, t: Turn) {
        if let Ok(()) = self.tx.send(SystemMessage::Turn(t)) {
            self.queue_depth.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn current_queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "#<Mailbox {}:{}>", self.actor_id, self.mailbox_id)
    }
}

impl std::hash::Hash for Mailbox {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mailbox_id.hash(state)
    }
}

impl Eq for Mailbox {}
impl PartialEq for Mailbox {
    fn eq(&self, other: &Mailbox) -> bool {
        self.mailbox_id == other.mailbox_id
    }
}

impl Ord for Mailbox {
    fn cmp(&self, other: &Mailbox) -> std::cmp::Ordering {
        return self.mailbox_id.cmp(&other.mailbox_id)
    }
}

impl PartialOrd for Mailbox {
    fn partial_cmp(&self, other: &Mailbox) -> Option<std::cmp::Ordering> {
        return Some(self.cmp(&other))
    }
}

impl Clone for Mailbox {
    fn clone(&self) -> Self {
        let Mailbox { actor_id, tx, queue_depth, mailbox_count, .. } = self;
        let _old_refcount = mailbox_count.fetch_add(1, Ordering::SeqCst);
        let new_mailbox = Mailbox {
            actor_id: *actor_id,
            mailbox_id: crate::next_mailbox_id(),
            tx: tx.clone(),
            queue_depth: Arc::clone(queue_depth),
            mailbox_count: Arc::clone(mailbox_count),
        };
        // tracing::trace!(old_mailbox = debug(&self),
        //                 new_mailbox = debug(&new_mailbox),
        //                 new_mailbox_refcount = debug(_old_refcount + 1));
        new_mailbox
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        let old_mailbox_refcount = self.mailbox_count.fetch_sub(1, Ordering::SeqCst);
        let new_mailbox_refcount = old_mailbox_refcount - 1;
        // tracing::trace!(mailbox = debug(&self),
        //                 new_mailbox_refcount);
        if new_mailbox_refcount == 0 {
            let _ = self.tx.send(SystemMessage::Release);
            ()
        }
    }
}

impl Actor {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        let actor_id = crate::next_actor_id();
        // tracing::trace!(id = actor_id, "Actor::new");
        Actor {
            actor_id,
            tx,
            rx: Some(rx),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            mailbox_count: Arc::new(AtomicUsize::new(0)),
            outbound_assertions: Map::new(),
            oid_map: Map::new(),
            next_task_id: 0,
            linked_tasks: Map::new(),
            exit_hooks: Vec::new(),
        }
    }

    pub fn create_and_start<E: Entity + Send + 'static>(name: tracing::Span, e: E) -> Arc<Ref> {
        Self::create_and_start_rec(name, e, |_, _, _| ())
    }

    pub fn create_and_start_rec<E: Entity + Send + 'static,
                                F: FnOnce(&mut Self, &mut E, &Arc<Ref>) -> ()>(
        name: tracing::Span,
        e: E,
        f: F,
    ) -> Arc<Ref> {
        let mut ac = Self::new();
        let r = ac.create_rec(e, f);
        ac.start(name);
        r
    }

    pub fn id(&self) -> ActorId {
        self.actor_id
    }

    fn mailbox(&mut self) -> Mailbox {
        let _old_refcount = self.mailbox_count.fetch_add(1, Ordering::SeqCst);
        let new_mailbox = Mailbox {
            actor_id: self.actor_id,
            mailbox_id: crate::next_mailbox_id(),
            tx: self.tx.clone(),
            queue_depth: Arc::clone(&self.queue_depth),
            mailbox_count: Arc::clone(&self.mailbox_count),
        };
        // tracing::trace!(new_mailbox = debug(&new_mailbox),
        //                 new_mailbox_refcount = debug(_old_refcount + 1));
        new_mailbox
    }

    pub fn shutdown(&mut self) {
        let _ = self.tx.send(SystemMessage::Release);
        ()
    }

    pub fn create<E: Entity + Send + 'static>(&mut self, e: E) -> Arc<Ref> {
        self.create_rec(e, |_, _, _| ())
    }

    pub fn create_rec<E: Entity + Send + 'static,
                      F: FnOnce(&mut Self, &mut E, &Arc<Ref>) -> ()>(
        &mut self,
        mut e: E,
        f: F,
    ) -> Arc<Ref> {
        let oid = crate::next_oid();
        let r = Arc::new(Ref {
            addr: Arc::new(ObjectAddress {
                mailbox: self.mailbox(),
                oid: oid.clone(),
            }),
            attenuation: Vec::new(),
        });
        f(self, &mut e, &r);
        self.oid_map.insert(oid, Box::new(e));
        r
    }

    pub fn boot<F: 'static + Send + for<'a> FnOnce(&'a mut Activation) -> BoxFuture<'a, ActorResult>>(
        mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorHandle {
        name.record("actor_id", &self.id());
        tokio::spawn(async move {
            tracing::trace!("start");
            {
                let queue_depth = Arc::clone(&self.queue_depth);
                self.linked_task(crate::name!("queue-monitor"), async move {
                    let mut timer = tokio::time::interval(core::time::Duration::from_secs(1));
                    loop {
                        timer.tick().await;
                        tracing::info!(queue_depth = debug(queue_depth.load(Ordering::Relaxed)));
                    }
                });
            }
            let result = self.run(boot).await;
            {
                let mut t = Activation::for_actor(&mut self);
                for r in std::mem::take(&mut t.actor.exit_hooks) {
                    match t.actor.oid_map.remove_entry(&r.addr.oid) {
                        None => (),
                        Some((k, mut e)) => {
                            if let Err(err) = e.exit_hook(&mut t, &result).await {
                                tracing::error!(err = debug(err),
                                                r = debug(&r),
                                                "error in exit hook");
                            }
                            t.actor.oid_map.insert(k, e);
                        }
                    }
                }
            }
            match &result {
                Ok(()) => {
                    tracing::trace!("normal stop");
                    ()
                }
                Err(e) => tracing::error!("error stop: {}", e),
            }
            result
        }.instrument(name))
    }

    pub fn start(self, name: tracing::Span) -> ActorHandle {
        self.boot(name, |_ac| Box::pin(ready(Ok(()))))
    }

    async fn run<F: 'static + Send + for<'a> FnOnce(&'a mut Activation) -> BoxFuture<'a, ActorResult>>(
        &mut self,
        boot: F,
    ) -> ActorResult {
        let _id = self.id();
        // tracing::trace!(_id, "boot");
        boot(&mut Activation::for_actor(self)).await?;
        // tracing::trace!(_id, "run");
        loop {
            match self.rx.as_mut().expect("present rx channel half").recv().await {
                None =>
                    Err(error("Unexpected channel close", _Any::new(false)))?,
                Some(m) => {
                    let should_stop = self.handle(m).await?;
                    if should_stop {
                        return Ok(());
                    }
                    // We would have a loop calling try_recv until it answers "no more at
                    // present" here, to avoid decrementing queue_depth for every message
                    // (instead zeroing it on queue empty - it only needs to be approximate),
                    // but try_recv has been removed from mpsc at the time of writing. See
                    // https://github.com/tokio-rs/tokio/issues/3350 . (***)
                }
            }
        }
    }

    pub fn add_exit_hook(&mut self, r: &Arc<Ref>) {
        self.exit_hooks.push(Arc::clone(r))
    }

    async fn handle(&mut self, m: SystemMessage) -> Result<bool, Error> {
        match m {
            SystemMessage::Release => {
                tracing::trace!("SystemMessage::Release");
                Ok(true)
            }
            SystemMessage::ReleaseOid(oid) => {
                tracing::trace!("SystemMessage::ReleaseOid({:?})", &oid);
                self.oid_map.remove(&oid);
                Ok(false)
            }
            SystemMessage::Turn(Turn(mut events)) => {
                let mut t = Activation::for_actor(self);
                loop {
                    for TurnEvent { oid, event } in events.into_iter() {
                        t.with_oid(&oid, |_| Ok(()), |t, e| match event {
                            Event::Assert(b) => {
                                let Assert { assertion: Assertion(assertion), handle } = *b;
                                e.assert(t, assertion, handle)
                            }
                            Event::Retract(b) => {
                                let Retract { handle } = *b;
                                e.retract(t, handle)
                            }
                            Event::Message(b) => {
                                let Message { body: Assertion(body) } = *b;
                                e.message(t, body)
                            }
                            Event::Sync(b) => {
                                let Sync { peer } = *b;
                                e.sync(t, peer)
                            }
                        })?;
                    }
                    events = std::mem::take(&mut t.immediate_self);
                    if events.is_empty() { break; }
                }
                t.actor.queue_depth.fetch_sub(1, Ordering::Relaxed); // see (***) in this file
                Ok(false)
            }
            SystemMessage::Crash(e) => {
                tracing::trace!("SystemMessage::Crash({:?})", &e);
                Err(e)?
            }
        }
    }

    pub fn linked_task<F: futures::Future<Output = ActorResult> + Send + 'static>(
        &mut self,
        name: tracing::Span,
        boot: F,
    ) {
        let mailbox = self.mailbox();
        let token = CancellationToken::new();
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        name.record("task_id", &task_id);
        {
            let token = token.clone();
            tokio::spawn(async move {
                tracing::trace!(task_id, "linked task start");
                select! {
                    _ = token.cancelled() => {
                        tracing::trace!(task_id, "linked task cancelled");
                        Ok(())
                    }
                    result = boot => {
                        match &result {
                            Ok(()) => {
                                tracing::trace!(task_id, "linked task normal stop");
                                ()
                            }
                            Err(e) => {
                                tracing::error!(task_id, "linked task error: {}", e);
                                let _ = mailbox.tx.send(SystemMessage::Crash(e.clone()));
                                ()
                            }
                        }
                        result
                    }
                }
            }.instrument(name));
        }
        self.linked_tasks.insert(task_id, token);
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        let mut rx = self.rx.take().expect("present rx channel half during drop");
        rx.close();

        for (_task_id, token) in std::mem::take(&mut self.linked_tasks).into_iter() {
            token.cancel();
        }

        let to_clear = std::mem::take(&mut self.outbound_assertions);
        {
            let mut t = Activation::for_actor(self);
            for (handle, r) in to_clear.into_iter() {
                tracing::trace!(h = debug(&handle), "retract on termination");
                t.retract_known_ref(r, handle);
            }
        }

        // In future, could do this:
        // tokio::spawn(async move {
        //     while let Some(m) = rx.recv().await {
        //         match m { ... }
        //     }
        // });

        tracing::trace!("Actor::drop");
    }
}

impl Ref {
    pub async fn external_event(&self, event: Event) {
        self.addr.mailbox.send(Turn(vec![TurnEvent { oid: self.addr.oid.clone(), event }]))
    }

    pub async fn external_events(&self, events: Vec<Event>) {
        self.addr.mailbox.send(Turn(events.into_iter().map(|event| TurnEvent {
            oid: self.addr.oid.clone(),
            event,
        }).collect()))
    }

    pub fn attenuate(&self, attenuation: &sturdy::Attenuation) -> Result<Arc<Self>, CaveatError> {
        let mut r = Ref {
            addr: Arc::clone(&self.addr),
            attenuation: self.attenuation.clone(),
        };
        r.attenuation.extend(attenuation.check()?);
        Ok(Arc::new(r))
    }

    pub fn rewrite(&self, mut a: _Any) -> Option<Assertion> {
        for c in &self.attenuation {
            match c.rewrite(&a) {
                Some(v) => a = v,
                None => return None,
            }
        }
        Some(Assertion(a))
    }
}

impl std::fmt::Debug for Ref {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if self.attenuation.is_empty() {
            write!(f, "⌜{}:{}⌝", self.addr.mailbox.actor_id, self.addr.oid.0)
        } else {
            write!(f, "⌜{}:{}\\{:?}⌝", self.addr.mailbox.actor_id, self.addr.oid.0, self.attenuation)
        }
    }
}

impl Drop for ObjectAddress {
    fn drop(&mut self) {
        let _ = self.mailbox.tx.send(SystemMessage::ReleaseOid(self.oid.clone()));
        ()
    }
}

impl Domain for Ref {}

impl std::convert::TryFrom<&IOValue> for Ref {
    type Error = preserves_schema::support::ParseError;
    fn try_from(_v: &IOValue) -> Result<Self, Self::Error> {
        panic!("Attempted to serialize Ref via IOValue");
    }
}

impl std::convert::From<&Ref> for IOValue {
    fn from(_v: &Ref) -> IOValue {
        panic!("Attempted to deserialize Ref via IOValue");
    }
}

#[macro_export]
macro_rules! name {
    () => {tracing::info_span!(actor_id = tracing::field::Empty,
                               task_id = tracing::field::Empty,
                               oid = tracing::field::Empty)};
    ($($item:tt)*) => {tracing::info_span!($($item)*,
                                           actor_id = tracing::field::Empty,
                                           task_id = tracing::field::Empty,
                                           oid = tracing::field::Empty)}
}
