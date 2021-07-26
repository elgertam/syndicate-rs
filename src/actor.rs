pub use futures::future::BoxFuture;

pub use std::future::ready;

use super::ActorId;
use super::schemas::sturdy;
use super::error::Error;
use super::error::encode_error;
use super::error::error;
use super::rewrite::CaveatError;
use super::rewrite::CheckedCaveat;

use preserves::value::Domain;
use preserves::value::IOValue;
use preserves::value::Map;
use preserves::value::NestedValue;
use preserves_schema::support::ParseError;

use std::boxed::Box;
use std::collections::hash_map::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

use tracing::Instrument;

pub use super::schemas::internal_protocol::_Any;
pub type Handle = u64;

pub type ActorResult = Result<(), Error>;
pub type ActorHandle = tokio::task::JoinHandle<ActorResult>;

pub struct Synced;

pub trait Entity<M>: Send {
    fn assert(&mut self, _t: &mut Activation, _a: M, _h: Handle) -> ActorResult {
        Ok(())
    }
    fn retract(&mut self, _t: &mut Activation, _h: Handle) -> ActorResult {
        Ok(())
    }
    fn message(&mut self, _t: &mut Activation, _m: M) -> ActorResult {
        Ok(())
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        t.message(&peer, Synced);
        Ok(())
    }
    fn turn_end(&mut self, _t: &mut Activation) -> ActorResult {
        Ok(())
    }
    fn exit_hook(&mut self, _t: &mut Activation, _exit_status: &Arc<ActorResult>) -> ActorResult {
        Ok(())
    }
}

pub struct InertEntity;
impl<M> Entity<M> for InertEntity {}

enum CleanupAction {
    ForMyself(Action),
    ForAnother(Arc<Mailbox>, Action),
}

type CleanupActions = Map<Handle, CleanupAction>;
pub type Action = Box<dyn Send + FnOnce(&mut Activation) -> ActorResult>;
pub type PendingEventQueue = Vec<Action>;

// This is what other implementations call a "Turn", renamed here to
// avoid conflicts with schemas::internal_protocol::Turn.
pub struct Activation<'activation> {
    pub actor: ActorRef,
    pub state: &'activation mut RunningActor,
    pending: EventBuffer,
}

struct EventBuffer {
    pub debtor: Arc<Debtor>,
    queues: HashMap<ActorId, (UnboundedSender<SystemMessage>, PendingEventQueue)>,
    for_myself: PendingEventQueue,
}

#[derive(Debug)]
pub struct Debtor {
    id: u64,
    debt: Arc<AtomicI64>,
    // notify: Notify,
}

#[derive(Debug)]
pub struct LoanedItem<T> {
    pub debtor: Arc<Debtor>,
    pub cost: usize,
    pub item: T,
}

enum SystemMessage {
    Release,
    Turn(LoanedItem<PendingEventQueue>),
    Crash(Error),
}

pub struct Mailbox {
    pub actor_id: ActorId,
    tx: UnboundedSender<SystemMessage>,
}

pub struct Actor {
    rx: UnboundedReceiver<SystemMessage>,
    ac_ref: ActorRef,
}

#[derive(Clone)]
pub struct ActorRef {
    pub actor_id: ActorId,
    state: Arc<Mutex<ActorState>>,
}

pub enum ActorState {
    Running(RunningActor),
    Terminated {
        exit_status: Arc<ActorResult>,
    },
}

pub struct RunningActor {
    pub actor_id: ActorId,
    tx: UnboundedSender<SystemMessage>,
    mailbox: Weak<Mailbox>,
    cleanup_actions: CleanupActions,
    next_task_id: u64,
    linked_tasks: Map<u64, CancellationToken>,
    exit_hooks: Vec<Box<dyn Send + FnOnce(&mut Activation, &Arc<ActorResult>) -> ActorResult>>,
}

pub struct Ref<M> {
    pub mailbox: Arc<Mailbox>,
    pub target: Mutex<Option<Box<dyn Entity<M>>>>,
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cap {
    pub underlying: Arc<Ref<_Any>>,
    pub attenuation: Vec<CheckedCaveat>,
}

pub struct Guard<M>
where
    for<'a> &'a M: Into<_Any>,
    for<'a> M: TryFrom<&'a _Any>,
{
    underlying: Arc<Ref<M>>
}

//---------------------------------------------------------------------------

static NEXT_DEBTOR_ID: AtomicU64 = AtomicU64::new(4);

preserves_schema::support::lazy_static! {
    pub static ref SYNDICATE_CREDIT: i64 = {
        let credit =
            std::env::var("SYNDICATE_CREDIT").unwrap_or("100".to_owned())
            .parse::<i64>().expect("Valid SYNDICATE_CREDIT environment variable");
        tracing::info!("Configured SYNDICATE_CREDIT = {}", credit);
        credit
    };

    pub static ref DEBTORS: RwLock<Map<u64, (tracing::Span, Arc<AtomicI64>)>> =
        RwLock::new(Map::new());
}

pub fn start_debt_reporter() {
    Actor::new().boot(crate::name!("debt-reporter"), |t| {
        t.state.linked_task(crate::name!("tick"), async {
            let mut timer = tokio::time::interval(core::time::Duration::from_secs(1));
            loop {
                timer.tick().await;
                for (id, (name, debt)) in DEBTORS.read().unwrap().iter() {
                    let _enter = name.enter();
                    tracing::info!(id, debt = debug(debt.load(Ordering::Relaxed)));
                }
            }
        });
        Ok(())
    });
}

impl TryFrom<&_Any> for Synced {
    type Error = ParseError;
    fn try_from(value: &_Any) -> Result<Self, Self::Error> {
        if let Some(true) = value.value().as_boolean() {
            Ok(Synced)
        } else {
            Err(ParseError::conformance_error("Synced"))
        }
    }
}

impl From<&Synced> for _Any {
    fn from(_value: &Synced) -> Self {
        _Any::new(true)
    }
}

impl<'activation> Activation<'activation> {
    fn make(actor: &ActorRef, debtor: Arc<Debtor>, state: &'activation mut RunningActor) -> Self {
        Activation {
            actor: actor.clone(),
            state,
            pending: EventBuffer::new(debtor),
        }
    }

    pub fn for_actor<F>(
        actor: &ActorRef,
        debtor: Arc<Debtor>,
        f: F,
    ) -> ActorResult where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        match Self::for_actor_exit(actor, debtor, |t| match f(t) {
            Ok(()) => None,
            Err(e) => Some(Err(e)),
        }) {
            None => Ok(()),
            Some(e) => Err(error("Could not activate terminated actor", encode_error(e))),
        }
    }

    pub fn for_actor_exit<F>(
        actor: &ActorRef,
        debtor: Arc<Debtor>,
        f: F,
    ) -> Option<ActorResult> where
        F: FnOnce(&mut Activation) -> Option<ActorResult>,
    {
        match actor.state.lock() {
            Err(_) => panicked_err(),
            Ok(mut g) => match &mut *g {
                ActorState::Terminated { exit_status } =>
                    Some((**exit_status).clone()),
                ActorState::Running(state) =>
                    match f(&mut Activation::make(actor, debtor, state)) {
                        None => None,
                        Some(exit_status) => {
                            let exit_status = Arc::new(exit_status);
                            let mut t = Activation::make(actor, Debtor::new(crate::name!("shutdown")), state);
                            for action in std::mem::take(&mut t.state.exit_hooks) {
                                if let Err(err) = action(&mut t, &exit_status) {
                                    tracing::error!(err = debug(err), "error in exit hook");
                                }
                            }
                            *g = ActorState::Terminated {
                                exit_status: Arc::clone(&exit_status),
                            };
                            Some((*exit_status).clone())
                        }
                    },
            }
        }
    }

    fn immediate_oid<M>(&self, r: &Arc<Ref<M>>) {
        if r.mailbox.actor_id != self.actor.actor_id {
            panic!("Cannot use for_myself to send to remote peers");
        }
    }

    pub fn assert<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        let handle = crate::next_handle();
        {
            let r = Arc::clone(r);
            self.pending.queue_for(&r).push(Box::new(
                move |t| r.with_entity(|e| e.assert(t, a, handle))));
        }
        {
            let r = Arc::clone(r);
            self.state.cleanup_actions.insert(
                handle,
                CleanupAction::ForAnother(Arc::clone(&r.mailbox), Box::new(
                    move |t| r.with_entity(|e| e.retract(t, handle)))));
        }
        handle
    }

    pub fn assert_for_myself<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        self.immediate_oid(r);
        let handle = crate::next_handle();
        {
            let r = Arc::clone(r);
            self.pending.for_myself.push(Box::new(
                move |t| r.with_entity(|e| e.assert(t, a, handle))));
        }
        {
            let r = Arc::clone(r);
            self.state.cleanup_actions.insert(
                handle,
                CleanupAction::ForMyself(Box::new(
                    move |t| r.with_entity(|e| e.retract(t, handle)))));
        }
        handle
    }

    pub fn retract(&mut self, handle: Handle) {
        if let Some(d) = self.state.cleanup_actions.remove(&handle) {
            self.pending.execute_cleanup_action(d)
        }
    }

    pub fn message<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, m: M) {
        let r = Arc::clone(r);
        self.pending.queue_for(&r).push(Box::new(
            move |t| r.with_entity(|e| e.message(t, m))))
    }

    pub fn message_for_myself<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, m: M) {
        self.immediate_oid(r);
        let r = Arc::clone(r);
        self.pending.for_myself.push(Box::new(
            move |t| r.with_entity(|e| e.message(t, m))))
    }

    pub fn sync<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, peer: Arc<Ref<Synced>>) {
        let r = Arc::clone(r);
        self.pending.queue_for(&r).push(Box::new(
            move |t| r.with_entity(|e| e.sync(t, peer))))
    }

    pub fn debtor(&self) -> &Arc<Debtor> {
        &self.pending.debtor
    }

    pub fn deliver(&mut self) {
        self.pending.deliver();
    }
}

impl EventBuffer {
    fn new(debtor: Arc<Debtor>) -> Self {
        EventBuffer {
            debtor,
            queues: HashMap::new(),
            for_myself: Vec::new(),
        }
    }

    fn execute_cleanup_action(&mut self, d: CleanupAction) {
        match d {
            CleanupAction::ForAnother(mailbox, action) =>
                self.queue_for_mailbox(&mailbox).push(action),
            CleanupAction::ForMyself(action) =>
                self.for_myself.push(action),
        }
    }

    fn queue_for<M>(&mut self, r: &Arc<Ref<M>>) -> &mut PendingEventQueue {
        self.queue_for_mailbox(&r.mailbox)
    }

    fn queue_for_mailbox(&mut self, mailbox: &Arc<Mailbox>) -> &mut PendingEventQueue {
        &mut self.queues.entry(mailbox.actor_id)
            .or_insert((mailbox.tx.clone(), Vec::new())).1
    }

    fn deliver(&mut self) {
        if !self.for_myself.is_empty() {
            panic!("Unprocessed for_myself events remain at deliver() time");
        }
        for (_actor_id, (tx, turn)) in std::mem::take(&mut self.queues).into_iter() {
            let _ = send_actions(&tx, &self.debtor, turn);
        }
    }
}

impl Drop for EventBuffer {
    fn drop(&mut self) {
        self.deliver()
    }
}

impl Debtor {
    pub fn new(name: tracing::Span) -> Arc<Self> {
        let id = NEXT_DEBTOR_ID.fetch_add(1, Ordering::Relaxed);
        let debt = Arc::new(AtomicI64::new(0));
        DEBTORS.write().unwrap().insert(id, (name, Arc::clone(&debt)));
        Arc::new(Debtor {
            id,
            debt,
            // notify: Notify::new(),
        })
    }

    pub fn balance(&self) -> i64 {
        self.debt.load(Ordering::Relaxed)
    }

    pub fn borrow(&self, token_count: usize) {
        let token_count: i64 = token_count.try_into().expect("manageable token count");
        self.debt.fetch_add(token_count, Ordering::Relaxed);
    }

    pub fn repay(&self, token_count: usize) {
        let token_count: i64 = token_count.try_into().expect("manageable token count");
        let _old_debt = self.debt.fetch_sub(token_count, Ordering::Relaxed);
        // if _old_debt - token_count <= *SYNDICATE_CREDIT {
        //     self.notify.notify_one();
        // }
    }

    pub async fn ensure_clear_funds(&self) {
        let limit = *SYNDICATE_CREDIT;
        tokio::task::yield_now().await;
        while self.balance() > limit {
            tokio::task::yield_now().await;
            // self.notify.notified().await;
        }
    }
}

impl Drop for Debtor {
    fn drop(&mut self) {
        DEBTORS.write().unwrap().remove(&self.id);
    }
}

impl<T> LoanedItem<T> {
    pub fn new(debtor: &Arc<Debtor>, cost: usize, item: T) -> Self {
        debtor.borrow(cost);
        LoanedItem { debtor: Arc::clone(debtor), cost, item }
    }
}

impl<T> Drop for LoanedItem<T> {
    fn drop(&mut self) {
        self.debtor.repay(self.cost);
    }
}

#[must_use]
fn send_actions(
    tx: &UnboundedSender<SystemMessage>,
    debtor: &Arc<Debtor>,
    t: PendingEventQueue,
) -> ActorResult {
    let token_count = t.len();
    tx.send(SystemMessage::Turn(LoanedItem::new(debtor, token_count, t)))
        .map_err(|_| error("Target actor not running", _Any::new(false)))
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "#<Mailbox {}>", self.actor_id)
    }
}

impl std::hash::Hash for Mailbox {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.actor_id.hash(state)
    }
}

impl Eq for Mailbox {}
impl PartialEq for Mailbox {
    fn eq(&self, other: &Mailbox) -> bool {
        self.actor_id == other.actor_id
    }
}

impl Ord for Mailbox {
    fn cmp(&self, other: &Mailbox) -> std::cmp::Ordering {
        return self.actor_id.cmp(&other.actor_id)
    }
}

impl PartialOrd for Mailbox {
    fn partial_cmp(&self, other: &Mailbox) -> Option<std::cmp::Ordering> {
        return Some(self.cmp(&other))
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        let _ = self.tx.send(SystemMessage::Release);
        ()
    }
}

impl Actor {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        let actor_id = crate::next_actor_id();
        // tracing::trace!(id = actor_id, "Actor::new");
        Actor {
            rx,
            ac_ref: ActorRef {
                actor_id,
                state: Arc::new(Mutex::new(ActorState::Running(RunningActor {
                    actor_id,
                    tx,
                    mailbox: Weak::new(),
                    cleanup_actions: Map::new(),
                    next_task_id: 0,
                    linked_tasks: Map::new(),
                    exit_hooks: Vec::new(),
                }))),
            },
        }
    }

    pub fn create_and_start<M, E: Entity<M> + Send + 'static>(
        name: tracing::Span,
        e: E,
    ) -> Arc<Ref<M>> {
        let r = Self::create_and_start_inert(name);
        r.become_entity(e);
        r
    }

    pub fn create_and_start_inert<M>(name: tracing::Span) -> Arc<Ref<M>> {
        let ac = Self::new();
        let r = ac.ac_ref.access(|s| s.unwrap().expect_running().create_inert());
        ac.start(name);
        r
    }

    pub fn boot<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorHandle {
        name.record("actor_id", &self.ac_ref.actor_id);
        tokio::spawn(async move {
            tracing::trace!("start");
            self.run(boot).await;
            let result = self.ac_ref.exit_status().expect("terminated");
            match &result {
                Ok(()) => tracing::trace!("normal stop"),
                Err(e) => tracing::error!("error stop: {}", e),
            }
            result
        }.instrument(name))
    }

    pub fn start(self, name: tracing::Span) -> ActorHandle {
        self.boot(name, |_ac| Ok(()))
    }

    fn terminate(&mut self, result: ActorResult) {
        let _ = Activation::for_actor_exit(
            &self.ac_ref, Debtor::new(crate::name!("shutdown")), |_| Some(result));
    }

    async fn run<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        boot: F,
    ) -> () {
        if Activation::for_actor(&self.ac_ref, Debtor::new(crate::name!("boot")), boot).is_err() {
            return;
        }

        loop {
            match self.rx.recv().await {
                None => {
                    return self.terminate(Err(error("Unexpected channel close", _Any::new(false))));
                }
                Some(m) => match m {
                    SystemMessage::Release => {
                        tracing::trace!("SystemMessage::Release");
                        return self.terminate(Ok(()));
                    }
                    SystemMessage::Turn(mut loaned_item) => {
                        let mut actions = std::mem::take(&mut loaned_item.item);
                        let r = Activation::for_actor(
                            &self.ac_ref, Arc::clone(&loaned_item.debtor), |t| {
                                loop {
                                    for action in actions.into_iter() { action(t)? }
                                    actions = std::mem::take(&mut t.pending.for_myself);
                                    if actions.is_empty() { break; }
                                }
                                Ok(())
                            });
                        if r.is_err() { return; }
                    }
                    SystemMessage::Crash(e) => {
                        tracing::trace!("SystemMessage::Crash({:?})", &e);
                        return self.terminate(Err(e));
                    }
                }
            }
        }
    }
}

fn panicked_err() -> Option<ActorResult> {
    Some(Err(error("Actor panicked", _Any::new(false))))
}

impl ActorRef {
    pub fn access<R, F: FnOnce(Option<&mut ActorState>) -> R>(&self, f: F) -> R {
        match self.state.lock() {
            Err(_) => f(None),
            Ok(mut g) => f(Some(&mut *g)),
        }
    }

    pub fn exit_status(&self) -> Option<ActorResult> {
        self.access(|s| s.map_or_else(
            panicked_err,
            |state| match state {
                ActorState::Running(_) => None,
                ActorState::Terminated { exit_status } => Some((**exit_status).clone()),
            }))
    }
}

impl ActorState {
    fn expect_running(&mut self) -> &mut RunningActor {
        match self {
            ActorState::Terminated { .. } => panic!("Expected a running actor"),
            ActorState::Running(r) => r,
        }
    }
}

impl RunningActor {
    pub fn shutdown(&self) {
        let _ = self.tx.send(SystemMessage::Release);
    }

    fn mailbox(&mut self) -> Arc<Mailbox> {
        match self.mailbox.upgrade() {
            None => {
                let new_mailbox = Arc::new(Mailbox {
                    actor_id: self.actor_id,
                    tx: self.tx.clone(),
                });
                self.mailbox = Arc::downgrade(&new_mailbox);
                new_mailbox
            }
            Some(m) => m
        }
    }

    pub fn inert_entity<M>(&mut self) -> Arc<Ref<M>> {
        self.create(InertEntity)
    }

    pub fn create<M, E: Entity<M> + Send + 'static>(&mut self, e: E) -> Arc<Ref<M>> {
        let r = self.create_inert();
        r.become_entity(e);
        r
    }

    pub fn create_inert<M>(&mut self) -> Arc<Ref<M>> {
        Arc::new(Ref {
            mailbox: self.mailbox(),
            target: Mutex::new(None),
        })
    }

    pub fn add_exit_hook<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>) {
        let r = Arc::clone(r);
        self.exit_hooks.push(Box::new(move |t, exit_status| {
            r.with_entity(|e| e.exit_hook(t, &exit_status))
        }))
    }

    pub fn linked_task<F: 'static + Send + futures::Future<Output = ActorResult>>(
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
        self.rx.close();
    }
}

impl Drop for RunningActor {
    fn drop(&mut self) {
        for (_task_id, token) in std::mem::take(&mut self.linked_tasks).into_iter() {
            token.cancel();
        }

        let to_clear = std::mem::take(&mut self.cleanup_actions);
        {
            let mut b = EventBuffer::new(Debtor::new(crate::name!("drop")));
            for (_handle, r) in to_clear.into_iter() {
                tracing::trace!(h = debug(&_handle), "retract on termination");
                b.execute_cleanup_action(r);
            }
        }

        tracing::trace!("Actor::drop");
    }
}

#[must_use]
pub fn external_event(mailbox: &Arc<Mailbox>, debtor: &Arc<Debtor>, action: Action) -> ActorResult {
    send_actions(&mailbox.tx, debtor, vec![action])
}

#[must_use]
pub fn external_events(mailbox: &Arc<Mailbox>, debtor: &Arc<Debtor>, events: PendingEventQueue) -> ActorResult {
    send_actions(&mailbox.tx, debtor, events)
}

impl<M> Ref<M> {
    pub fn become_entity<E: 'static + Entity<M>>(&self, e: E) {
        let mut g = self.target.lock().expect("unpoisoned");
        if g.is_some() {
            panic!("Double initialization of Ref");
        }
        *g = Some(Box::new(e));
    }

    pub fn with_entity<R, F: FnOnce(&mut dyn Entity<M>) -> R>(&self, f: F) -> R {
        let mut g = self.target.lock().expect("unpoisoned");
        f(g.as_mut().expect("initialized").as_mut())
    }
}

impl<M> Ref<M> {
    pub fn oid(&self) -> usize {
        std::ptr::addr_of!(*self) as usize
    }
}

impl<M> PartialEq for Ref<M> {
    fn eq(&self, other: &Self) -> bool {
        self.oid() == other.oid()
    }
}

impl<M> Eq for Ref<M> {}

impl<M> std::hash::Hash for Ref<M> {
    fn hash<H>(&self, hash: &mut H) where H: std::hash::Hasher {
        self.oid().hash(hash)
    }
}

impl<M> PartialOrd for Ref<M> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<M> Ord for Ref<M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.oid().cmp(&other.oid())
    }
}

impl<M> std::fmt::Debug for Ref<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "⌜{}:{:016x}⌝", self.mailbox.actor_id, self.oid())
    }
}

impl Cap {
    pub fn guard<M: 'static + Send>(underlying: &Arc<Ref<M>>) -> Arc<Self>
    where
        for<'a> &'a M: Into<_Any>,
        for<'a> M: TryFrom<&'a _Any>,
    {
        Self::new(&Arc::new(Ref {
            mailbox: Arc::clone(&underlying.mailbox),
            target: Mutex::new(Some(Box::new(Guard { underlying: underlying.clone() }))),
        }))
    }

    pub fn new(underlying: &Arc<Ref<_Any>>) -> Arc<Self> {
        Arc::new(Cap {
            underlying: Arc::clone(underlying),
            attenuation: Vec::new(),
        })
    }

    pub fn attenuate(&self, attenuation: &sturdy::Attenuation) -> Result<Arc<Self>, CaveatError> {
        let mut r = Cap { attenuation: self.attenuation.clone(), .. self.clone() };
        r.attenuation.extend(attenuation.check()?);
        Ok(Arc::new(r))
    }

    pub fn rewrite(&self, mut a: _Any) -> Option<_Any> {
        for c in &self.attenuation {
            match c.rewrite(&a) {
                Some(v) => a = v,
                None => return None,
            }
        }
        Some(a)
    }

    pub fn assert<M: Into<_Any>>(&self, t: &mut Activation, m: M) -> Option<Handle> {
        self.rewrite(m.into()).map(|m| t.assert(&self.underlying, m))
    }

    pub fn message<M: Into<_Any>>(&self, t: &mut Activation, m: M) {
        if let Some(m) = self.rewrite(m.into()) {
            t.message(&self.underlying, m)
        }
    }
}

impl std::fmt::Debug for Cap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if self.attenuation.is_empty() {
            self.underlying.fmt(f)
        } else {
            write!(f, "⌜{}:{:016x}\\{:?}⌝",
                   self.underlying.mailbox.actor_id,
                   self.underlying.oid(),
                   self.attenuation)
        }
    }
}

impl Domain for Cap {}

impl std::convert::TryFrom<&IOValue> for Cap {
    type Error = preserves_schema::support::ParseError;
    fn try_from(_v: &IOValue) -> Result<Self, Self::Error> {
        panic!("Attempted to serialize Cap via IOValue");
    }
}

impl std::convert::From<&Cap> for IOValue {
    fn from(_v: &Cap) -> IOValue {
        panic!("Attempted to deserialize Ref via IOValue");
    }
}

impl<M> Entity<_Any> for Guard<M>
where
    for<'a> &'a M: Into<_Any>,
    for<'a> M: TryFrom<&'a _Any>,
{
    fn assert(&mut self, t: &mut Activation, a: _Any, h: Handle) -> ActorResult {
        match M::try_from(&a) {
            Ok(a) => self.underlying.with_entity(|e| e.assert(t, a, h)),
            Err(_) => Ok(()),
        }
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        self.underlying.with_entity(|e| e.retract(t, h))
    }
    fn message(&mut self, t: &mut Activation, m: _Any) -> ActorResult {
        match M::try_from(&m) {
            Ok(m) => self.underlying.with_entity(|e| e.message(t, m)),
            Err(_) => Ok(()),
        }
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        self.underlying.with_entity(|e| e.sync(t, peer))
    }
    fn turn_end(&mut self, t: &mut Activation) -> ActorResult {
        self.underlying.with_entity(|e| e.turn_end(t))
    }
    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        self.underlying.with_entity(|e| e.exit_hook(t, exit_status))
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
