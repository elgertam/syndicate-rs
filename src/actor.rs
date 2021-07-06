pub use futures::future::BoxFuture;

pub use std::future::ready;

use super::Assertion;
use super::ActorId;
use super::Handle;
use super::schemas::internal_protocol::*;
use super::error::Error;

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

use tracing::{Instrument, trace, error};

pub type ActorResult = Result<(), Error>;
pub type ActorHandle = tokio::task::JoinHandle<ActorResult>;

pub trait Entity: Send {
    fn assert(&mut self, _t: &mut Activation, _a: Assertion, _h: Handle) -> ActorResult {
        Ok(())
    }
    fn retract(&mut self, _t: &mut Activation, _h: Handle) -> ActorResult {
        Ok(())
    }
    fn message(&mut self, _t: &mut Activation, _m: Assertion) -> ActorResult {
        Ok(())
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref>) -> ActorResult {
        t.message(peer, Assertion::new(true));
        Ok(())
    }
    fn turn_end(&mut self, _t: &mut Activation) -> ActorResult {
        Ok(())
    }
    fn exit_hook(&mut self, _t: &mut Activation, _exit_status: &ActorResult) -> BoxFuture<ActorResult> {
        Box::pin(ready(Ok(())))
    }
}

type OutboundAssertions = Map<Handle, Arc<Ref>>;

// This is what other implementations call a "Turn", renamed here to
// avoid conflicts with schemas::internal_protocol::Turn.
pub struct Activation<'activation> {
    pub actor: &'activation mut Actor,
    queues: HashMap<ActorId, Vec<(Arc<Ref>, Event)>>,
    turn_end_revisit_flag: bool,
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
    rx: UnboundedReceiver<SystemMessage>,
    queue_depth: Arc<AtomicUsize>,
    mailbox_count: Arc<AtomicUsize>,
    outbound_assertions: OutboundAssertions,
    oid_map: Map<Oid, Box<dyn Entity + Send>>,
    next_task_id: u64,
    linked_tasks: Map<u64, CancellationToken>,
    exit_hooks: Vec<Oid>,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ref {
    pub relay: Mailbox,
    pub target: Oid,
    /* TODO: attenuation */
}

//---------------------------------------------------------------------------

impl<'activation> Activation<'activation> {
    pub fn for_actor(actor: &'activation mut Actor) -> Self {
        Activation {
            actor,
            queues: HashMap::new(),
            turn_end_revisit_flag: false,
        }
    }

    pub fn assert<M>(&mut self, r: Arc<Ref>, a: M) -> Handle where M: Into<Assertion> {
        let handle = crate::next_handle();
        self.queue_for(&r).push((Arc::clone(&r), Event::Assert(Box::new(
            Assert { assertion: Assertion(a.into()), handle: handle.clone() }))));
        self.actor.outbound_assertions.insert(handle.clone(), r);
        handle
    }

    pub fn retract(&mut self, handle: Handle) {
        if let Some(r) = self.actor.outbound_assertions.remove(&handle) {
            self.retract_known_ref(r, handle)
        }
    }

    pub fn retract_known_ref(&mut self, r: Arc<Ref>, handle: Handle) {
        self.queue_for(&r).push((r, Event::Retract(Box::new(Retract { handle }))));
    }

    pub fn message<M>(&mut self, r: Arc<Ref>, m: M) where M: Into<Assertion> {
        self.queue_for(&r).push((r, Event::Message(Box::new(
            Message { body: Assertion(m.into()) }))))
    }

    pub fn set_turn_end_flag(&mut self) {
        self.turn_end_revisit_flag = true;
    }

    fn queue_for(&mut self, r: &Arc<Ref>) -> &mut Vec<(Arc<Ref>, Event)> {
        self.queues.entry(r.relay.actor_id).or_default()
    }

    fn deliver(&mut self) {
        for (_actor_id, turn) in std::mem::take(&mut self.queues).into_iter() {
            if turn.len() == 0 { continue; }
            let first_ref = Arc::clone(&turn[0].0);
            let target = &first_ref.relay;
            target.send(Turn(turn.into_iter().map(
                |(r, e)| TurnEvent { oid: r.target.clone(), event: e }).collect()));
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
        let _ = self.tx.send(SystemMessage::Turn(t));
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
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
        trace!(old_mailbox = debug(&self),
               new_mailbox = debug(&new_mailbox),
               new_mailbox_refcount = debug(_old_refcount + 1));
        new_mailbox
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        let old_mailbox_refcount = self.mailbox_count.fetch_sub(1, Ordering::SeqCst);
        let new_mailbox_refcount = old_mailbox_refcount - 1;
        trace!(mailbox = debug(&self),
               new_mailbox_refcount);
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
        trace!(id = actor_id, "Actor::new");
        Actor {
            actor_id,
            tx,
            rx,
            queue_depth: Arc::new(AtomicUsize::new(0)),
            mailbox_count: Arc::new(AtomicUsize::new(0)),
            outbound_assertions: Map::new(),
            oid_map: Map::new(),
            next_task_id: 0,
            linked_tasks: Map::new(),
            exit_hooks: Vec::new(),
        }
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
        trace!(new_mailbox = debug(&new_mailbox),
               new_mailbox_refcount = debug(_old_refcount + 1));
        new_mailbox
    }

    pub fn shutdown(&mut self) {
        let _ = self.tx.send(SystemMessage::Release);
        ()
    }

    pub fn create<E: Entity + Send + 'static>(&mut self, e: E) -> Arc<Ref> {
        let r = Ref {
            relay: self.mailbox(),
            target: crate::next_oid(),
        };
        self.oid_map.insert(r.target.clone(), Box::new(e));
        Arc::new(r)
    }

    pub fn boot<F: 'static + Send + FnOnce(&mut Self) -> BoxFuture<ActorResult>>(
        mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorHandle {
        let id = self.id();
        tokio::spawn(async move {
            trace!(id, "start");
            let result = self.run(boot).await;
            {
                let mut t = Activation::for_actor(&mut self);
                for oid in std::mem::take(&mut t.actor.exit_hooks) {
                    match t.actor.oid_map.remove_entry(&oid) {
                        None => (),
                        Some((k, mut e)) => {
                            if let Err(err) = e.exit_hook(&mut t, &result).await {
                                tracing::warn!(err = debug(err),
                                               oid = debug(oid),
                                               "error in exit hook");
                            }
                            t.actor.oid_map.insert(k, e);
                        }
                    }
                }
            }
            match &result {
                Ok(()) => trace!(id, "normal stop"),
                Err(e) => error!(id, "error stop: {}", e),
            }
            result
        }.instrument(name))
    }

    pub fn start(self, name: tracing::Span) -> ActorHandle {
        self.boot(name, |_ac| Box::pin(ready(Ok(()))))
    }

    async fn run<F: 'static + Send + FnOnce(&mut Self) -> BoxFuture<ActorResult>>(
        &mut self,
        boot: F,
    ) -> ActorResult {
        let id = self.id();
        trace!(id, "boot");
        boot(self).await?;
        trace!(id, "run");
        loop {
            match self.rx.recv().await {
                None =>
                    Err(Error {
                        message: "Unexpected channel close".to_owned(),
                        detail: _Any::new(false),
                    })?,
                Some(m) => {
                    trace!(id, m = debug(&m), "system message");
                    if self.handle(m)? {
                        return Ok(());
                    }
                    // We would have a loop calling try_recv until it answers "no more at
                    // present" here, to avoid decrementing queue_depth for every message
                    // (instead zeroing it on queue empty - it only needs to be approximate),
                    // but try_recv has been removed from mpsc at the time of writing. See
                    // https://github.com/tokio-rs/tokio/issues/3350 .
                    self.queue_depth.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn add_exit_hook(&mut self, oid: &Oid) {
        self.exit_hooks.push(oid.clone())
    }

    fn handle(&mut self, m: SystemMessage) -> Result<bool, Error> {
        match m {
            SystemMessage::Release =>
                Ok(true),
            SystemMessage::ReleaseOid(oid) => {
                self.oid_map.remove(&oid);
                Ok(false)
            }
            SystemMessage::Turn(Turn(events)) => {
                let mut t = Activation::for_actor(self);
                let mut revisit_oids = Vec::new();
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
                    if t.turn_end_revisit_flag {
                        t.turn_end_revisit_flag = false;
                        revisit_oids.push(oid);
                    }
                }
                for oid in revisit_oids {
                    t.with_oid(&oid, |_| Ok(()), |t, e| e.turn_end(t))?;
                }
                Ok(false)
            }
            SystemMessage::Crash(e) =>
                Err(e)?
        }
    }

    pub fn linked_task<F: futures::Future<Output = ActorResult> + Send + 'static>(
        &mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorHandle {
        let id = self.id();
        let mailbox = self.mailbox();
        let token = CancellationToken::new();
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        let handle = {
            let token = token.clone();
            tokio::spawn(async move {
                trace!(id, task_id, "linked task start");
                select! {
                    _ = token.cancelled() => {
                        trace!(id, task_id, "linked task cancelled");
                        Ok(())
                    }
                    result = boot => {
                        match &result {
                            Ok(()) => trace!(id, task_id, "linked task normal stop"),
                            Err(e) => {
                                error!(id, task_id, "linked task error: {}", e);
                                let _ = mailbox.tx.send(SystemMessage::Crash(e.clone()));
                                ()
                            }
                        }
                        result
                    }
                }
            }.instrument(name))
        };
        self.linked_tasks.insert(task_id, token);
        handle
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        for (_task_id, token) in std::mem::take(&mut self.linked_tasks).into_iter() {
            token.cancel();
        }

        let to_clear = std::mem::take(&mut self.outbound_assertions);
        let mut t = Activation::for_actor(self);
        for (handle, r) in to_clear.into_iter() {
            t.retract_known_ref(r, handle);
        }
    }
}

impl Ref {
    pub fn external_event(&self, event: Event) {
        self.relay.send(Turn(vec![TurnEvent { oid: self.target.clone(), event }]))
    }
}

impl Drop for Ref {
    fn drop(&mut self) {
        let _ = self.relay.tx.send(SystemMessage::ReleaseOid(self.target.clone()));
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
