use super::Assertion;
use super::ActorId;
use super::Handle;
use super::schemas::internal_protocol::*;
use super::error::Error;

use preserves::value::Domain;
use preserves::value::IOResult;
use preserves::value::IOValue;
use preserves::value::Map;
use preserves::value::NestedValue;

use std::boxed::Box;
use std::cell::Cell;
use std::collections::hash_map::HashMap;
use std::future::Future;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

use tracing::{Instrument, trace, error};

pub type ActorResult = Result<(), Error>;
pub type ActorHandle = tokio::task::JoinHandle<ActorResult>;

pub trait Entity {
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
}

type OutboundAssertions = Map<Handle, Arc<Ref>>;

// This is what other implementations call a "Turn", renamed here to
// avoid conflicts with schemas::internal_protocol::Turn.
pub struct Activation<'activation> {
    outbound_assertions: &'activation mut OutboundAssertions,
    queues: HashMap<ActorId, Vec<(Arc<Ref>, Event)>>,
}

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
    pub queue_depth: Arc<AtomicUsize>,
    pub mailbox_count: Arc<AtomicUsize>,
}

pub struct Actor {
    pub template_mailbox: Mailbox,
    rx: UnboundedReceiver<SystemMessage>,
    pub outbound_assertions: OutboundAssertions,
    pub oid_map: Map<Oid, Cell<Box<dyn Entity + Send>>>,
    pub next_task_id: u64,
    pub linked_tasks: Map<u64, CancellationToken>,
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
        Self::for_actor_details(&mut actor.outbound_assertions)
    }

    pub fn for_actor_details(outbound_assertions: &'activation mut OutboundAssertions) -> Self {
        Activation {
            outbound_assertions,
            queues: HashMap::new(),
        }
    }

    pub fn assert<M>(&mut self, r: Arc<Ref>, a: M) -> Handle where M: Into<Assertion> {
        let handle = crate::next_handle();
        self.queue_for(&r).push((Arc::clone(&r), Event::Assert(Box::new(
            Assert { assertion: Assertion(a.into()), handle: handle.clone() }))));
        self.outbound_assertions.insert(handle.clone(), r);
        handle
    }

    pub fn retract(&mut self, handle: Handle) {
        if let Some(r) = self.outbound_assertions.remove(&handle) {
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

    fn queue_for(&mut self, r: &Arc<Ref>) -> &mut Vec<(Arc<Ref>, Event)> {
        self.queues.entry(r.relay.actor_id).or_default()
    }

    pub fn deliver(&mut self) {
        for (_actor_id, turn) in std::mem::take(&mut self.queues).into_iter() {
            if turn.len() == 0 { continue; }
            let first_ref = Arc::clone(&turn[0].0);
            let target = &first_ref.relay;
            target.send(Turn(turn.into_iter().map(
                |(r, e)| TurnEvent { oid: r.target.clone(), event: e }).collect()));
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
        mailbox_count.fetch_add(1, Ordering::SeqCst);
        Mailbox {
            actor_id: *actor_id,
            mailbox_id: crate::next_mailbox_id(),
            tx: tx.clone(),
            queue_depth: Arc::clone(queue_depth),
            mailbox_count: Arc::clone(mailbox_count),
        }
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        if self.mailbox_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ = self.tx.send(SystemMessage::Release);
            ()
        }
    }
}

impl Actor {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        Actor {
            template_mailbox: Mailbox {
                actor_id: crate::next_actor_id(),
                mailbox_id: crate::next_mailbox_id(),
                tx,
                queue_depth: Arc::new(AtomicUsize::new(0)),
                mailbox_count: Arc::new(AtomicUsize::new(0)),
            },
            rx,
            outbound_assertions: Map::new(),
            oid_map: Map::new(),
            next_task_id: 0,
            linked_tasks: Map::new(),
        }
    }

    pub fn id(&self) -> ActorId {
        self.template_mailbox.actor_id
    }

    pub fn create<E: Entity + Send + 'static>(&mut self, e: E) -> Arc<Ref> {
        let r = Ref {
            relay: self.template_mailbox.clone(),
            target: crate::next_oid(),
        };
        self.oid_map.insert(r.target.clone(), Cell::new(Box::new(e)));
        Arc::new(r)
    }

    pub fn boot<F: Future<Output = ActorResult> + Send + 'static>(
        mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorHandle {
        tokio::spawn(async move {
            trace!("start");
            let run_future = self.run(boot);
            let result = run_future.await;
            match &result {
                Ok(()) => trace!("normal stop"),
                Err(e) => error!("{}", e),
            }
            result
        }.instrument(name))
    }

    pub fn start(self, name: tracing::Span) -> ActorHandle {
        self.boot(name, ready(Ok(())))
    }

    async fn run<F: Future<Output = ActorResult>>(&mut self, boot: F) -> ActorResult {
        boot.await?;
        loop {
            match self.rx.recv().await {
                None =>
                    Err(Error {
                        message: "Unexpected channel close".to_owned(),
                        detail: _Any::new(false),
                    })?,
                Some(m) => {
                    if self.handle(m)? {
                        return Ok(());
                    }
                    // We would have a loop calling try_recv until it answers "no more at
                    // present" here, to avoid decrementing queue_depth for every message
                    // (instead zeroing it on queue empty - it only needs to be approximate),
                    // but try_recv has been removed from mpsc at the time of writing. See
                    // https://github.com/tokio-rs/tokio/issues/3350 .
                    self.template_mailbox.queue_depth.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
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
                for TurnEvent { oid, event } in events.into_iter() {
                    if let Some(e) = self.oid_map.get_mut(&oid) {
                        let mut t = Activation::for_actor_details(&mut self.outbound_assertions);
                        let e = e.get_mut();
                        match event {
                            Event::Assert(b) => {
                                let Assert { assertion: Assertion(assertion), handle } = *b;
                                e.assert(&mut t, assertion, handle)?;
                            }
                            Event::Retract(b) => {
                                let Retract { handle } = *b;
                                e.retract(&mut t, handle)?;
                            }
                            Event::Message(b) => {
                                let Message { body: Assertion(body) } = *b;
                                e.message(&mut t, body)?;
                            }
                            Event::Sync(b) => {
                                let Sync { peer } = *b;
                                e.sync(&mut t, peer)?;
                            }
                        }
                    }
                }
                Ok(false)
            }
            SystemMessage::Crash(e) =>
                Err(e)?
        }
    }

    pub fn linked_task<F: Future<Output = ActorResult> + Send + 'static>(
        &mut self,
        name: tracing::Span,
        boot: F,
    ) {
        let mailbox = self.template_mailbox.clone();
        let token = CancellationToken::new();
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        {
            let token = token.clone();
            tokio::spawn(async move {
                trace!("linked task start");
                select! {
                    _ = token.cancelled() => (),
                    result = boot => match result {
                        Ok(()) => trace!("linked task normal stop"),
                        Err(e) => {
                            error!("linked task error: {}", e);
                            let _ = mailbox.tx.send(SystemMessage::Crash(e));
                            ()
                        }
                    }
                }
            }.instrument(name));
        }
        self.linked_tasks.insert(task_id, token);
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

impl Drop for Ref {
    fn drop(&mut self) {
        let _ = self.relay.tx.send(SystemMessage::ReleaseOid(self.target.clone()));
        ()
    }
}

impl Domain for Ref {
    fn from_preserves(v: IOValue) -> IOResult<Self> {
        panic!("aiee")
    }
    fn as_preserves(&self) -> IOValue {
        panic!("aiee")
    }
}

impl Domain for super::schemas::sturdy::WireRef {
    fn from_preserves(v: IOValue) -> IOResult<Self> {
        panic!("aiee")
    }
    fn as_preserves(&self) -> IOValue {
        panic!("aiee")
    }
}
