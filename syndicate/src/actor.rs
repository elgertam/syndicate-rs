#![doc = concat!(
    include_str!("../doc/actor.md"),
    include_str!("../doc/what-is-an-actor.md"),
    include_str!("../doc/flow-control.md"),
    include_str!("../doc/linked-tasks.md"),
)]

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
use tokio::sync::Notify;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

use tracing::Instrument;

/// The type of messages and assertions that can be exchanged among
/// distributed objects, including via [dataspace][crate::dataspace].
///
/// A Preserves value where embedded references are instances of
/// [`Cap`].
///
/// While [`Ref<M>`] can be used within a process, where arbitrary
/// `M`-values can be exchanged among objects, for distributed or
/// polyglot systems a *lingua franca* has to be chosen. `AnyValue` is
/// that language.
pub type AnyValue = super::schemas::internal_protocol::_Any;

/// The type of process-unique actor IDs.
pub type ActorId = u64;

/// The type of process-unique assertion handles.
///
/// Used both as a reference to [retract][Entity::retract]
/// previously-asserted assertions and as an indexing key to associate
/// local state with some incoming assertion in an entity.
pub type Handle = u64;

/// Responses to events must have type `ActorResult`.
pub type ActorResult = Result<(), Error>;

/// Methods [`Actor::boot`] and [`Actor::start`] return an
/// `ActorHandle`, representing the actor's mainloop task.
pub type ActorHandle = tokio::task::JoinHandle<ActorResult>;

/// A small protocol for indicating successful synchronisation with
/// some peer; see [Entity::sync].
pub struct Synced;

/// The core metaprotocol implemented by every object.
///
/// Entities communicate with each other by asserting and retracting
/// values and by sending messages (which can be understood very
/// approximately as "infinitesimally brief" assertions of the message
/// body).
///
/// Every assertion placed at a receiving entity *R* from some sending
/// entity *S* lives so long as *S*'s actor survives, *R*'s actor
/// survives, and *S* does not retract the assertion. Messages, by
/// contrast, are transient.
///
/// Implementors of [`Entity`] accept assertions from peers in method
/// [`assert`][Entity::assert]; notification of retraction of a
/// previously-asserted value happens in method
/// [`retract`][Entity::retract]; and notification of a message in
/// method [`message`][Entity::message].
///
/// In addition, entities may *synchronise* with each other: the
/// [`sync`][Entity::sync] method responds to a synchronisation
/// request.
///
/// Finally, the Rust implementation of the Syndicated Actor model
/// offers a hook for running some code at the end of an Entity's
/// containing [`Actor`]'s lifetime
/// ([`exit_hook`][Entity::exit_hook]).
///
/// # What to implement
///
/// The default implementations of the methods here generally do
/// nothing; override them to add actual behaviour to your entity.
///
#[allow(unused_variables)]
pub trait Entity<M>: Send {
    /// Receive notification of a new assertion from a peer.
    ///
    /// The `turn` parameter represents the current
    /// [activation][Activation]; `assertion` is the value (of type
    /// `M`) asserted; and `handle` is the process-unique name for
    /// this particular assertion instance that will be used later
    /// when it is [retracted][Entity::retract].
    ///
    /// The default implementation does nothing.
    fn assert(&mut self, turn: &mut Activation, assertion: M, handle: Handle) -> ActorResult {
        Ok(())
    }

    /// Receive notification of retraction of a previous assertion from a peer.
    ///
    /// This happens either when the asserting peer explicitly
    /// retracts an assertion, or when its animating [`Actor`]
    /// terminates.
    ///
    /// The `turn` parameter represents the current
    /// [activation][Activation], and `handle` is the process-unique
    /// name for this particular assertion instance being retracted.
    ///
    /// Note that no `assertion` value is provided: entities needing
    /// to know the value that was previously asserted must remember
    /// it themselves (perhaps in a [`Map`] keyed by `handle`).
    ///
    /// The default implementation does nothing.
    fn retract(&mut self, turn: &mut Activation, handle: Handle) -> ActorResult {
        Ok(())
    }

    /// Receive notification of a message from a peer.
    ///
    /// The `turn` parameter represents the current
    /// [activation][Activation], and `message` is the body of the
    /// message sent.
    ///
    /// The default implementation does nothing.
    fn message(&mut self, turn: &mut Activation, message: M) -> ActorResult {
        Ok(())
    }

    /// Respond to a synchronisation request from a peer.
    ///
    /// Implementors of [`Entity`] will seldom override this. The
    /// default implementation fulfils the synchronisation protocol by
    /// responding to `peer` with a `Synced` message.
    ///
    /// In special cases, for example when an entity is a proxy for
    /// some remote entity, the right thing to do is to forward the
    /// synchronisation request on to another entity; in those cases,
    /// overriding the default behaviour is appropriate.
    fn sync(&mut self, turn: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        turn.message(&peer, Synced);
        Ok(())
    }

    /// Optional callback for running cleanup actions when the
    /// entity's animating [Actor] terminates.
    ///
    /// Programs register an entity's exit hook with
    /// [RunningActor::add_exit_hook].
    ///
    /// The default implementation does nothing.
    fn exit_hook(&mut self, turn: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        Ok(())
    }
}

/// An "inert" entity, that does nothing in response to any event delivered to it.
///
/// Useful as a placeholder or dummy in various situations.
pub struct InertEntity;
impl<M> Entity<M> for InertEntity {}

enum CleanupAction {
    ForMyself(Action),
    ForAnother(Arc<Mailbox>, Action),
}

type CleanupActions = Map<Handle, CleanupAction>;

type Action = Box<dyn Send + FnOnce(&mut Activation) -> ActorResult>;

#[doc(hidden)]
pub type PendingEventQueue = Vec<Action>;

/// The main API for programming Syndicated Actor objects.
///
/// Through `Activation`s, programs can access the state of their
/// animating [`RunningActor`].
///
/// Many actions that an entity can perform are methods directly on
/// `Activation`, but methods on the [`RunningActor`] and [`ActorRef`]
/// values contained in an `Activation` are also sometimes useful.
///
/// This is what other implementations call a "Turn", renamed here to
/// avoid conflicts with [`crate::schemas::internal_protocol::Turn`].
pub struct Activation<'activation> {
    /// A reference to the implementation-side of the currently active [`Actor`].
    pub actor: ActorRef,
    /// A reference to the current state of the active [`Actor`].
    pub state: &'activation mut RunningActor,
    pending: EventBuffer,
}

struct EventBuffer {
    pub account: Arc<Account>,
    queues: HashMap<ActorId, (UnboundedSender<SystemMessage>, PendingEventQueue)>,
    for_myself: PendingEventQueue,
}

/// An `Account` records a "debt" in terms of outstanding work items.
///
/// It is part of the flow control mechanism - see [the module-level
/// documentation][crate::actor#flow-control] for more.
#[derive(Debug)]
pub struct Account {
    id: u64,
    debt: Arc<AtomicI64>,
    notify: Notify,
}

/// A `LoanedItem<T>` is a `T` with an associated `cost` recorded
/// against it in the ledger of a given [`Account`]. The cost is
/// repaid automatically when the `LoanedItem<T>` is `Drop`ped.
///
/// `LoanedItem`s are part of the flow control mechanism - see [the
/// module-level documentation][crate::actor#flow-control] for more.
#[derive(Debug)]
pub struct LoanedItem<T> {
    /// The account against which this loan is recorded.
    pub account: Arc<Account>,
    /// The cost of this particular `T`.
    pub cost: usize,
    /// The underlying item itself.
    pub item: T,
}

enum SystemMessage {
    Release,
    Turn(LoanedItem<PendingEventQueue>),
    Crash(Error),
}

/// The mechanism by which events are delivered to a given [`Actor`].
pub struct Mailbox {
    /// The ID of the actor this mailbox corresponds to.
    pub actor_id: ActorId,
    tx: UnboundedSender<SystemMessage>,
}

/// Each actor owns an instance of this structure.
///
/// It holds the receive-half of the actor's mailbox, plus a reference
/// to the actor's private state.
pub struct Actor {
    rx: UnboundedReceiver<SystemMessage>,
    ac_ref: ActorRef,
}

/// A reference to an actor's private [`ActorState`].
#[derive(Clone)]
pub struct ActorRef {
    /// The ID of the referenced actor.
    pub actor_id: ActorId,
    state: Arc<Mutex<ActorState>>,
}

/// The state of an actor: either `Running` or `Terminated`.
pub enum ActorState {
    /// A non-terminated actor has an associated [`RunningActor`] state record.
    Running(RunningActor),
    /// A terminated actor has an [`ActorResult`] as its `exit_status`.
    Terminated {
        /// The exit status of the actor: `Ok(())` for normal
        /// termination, `Err(_)` for abnormal termination.
        exit_status: Arc<ActorResult>,
    },
}

/// State associated with each non-terminated [`Actor`].
pub struct RunningActor {
    /// The ID of the actor this state belongs to.
    pub actor_id: ActorId,
    tx: UnboundedSender<SystemMessage>,
    mailbox: Weak<Mailbox>,
    cleanup_actions: CleanupActions,
    next_task_id: u64,
    linked_tasks: Map<u64, CancellationToken>,
    exit_hooks: Vec<Box<dyn Send + FnOnce(&mut Activation, &Arc<ActorResult>) -> ActorResult>>,
}

/// A reference to an object that expects messages/assertions of type
/// `M`.
///
/// The object can be in the same actor, in a different local
/// (in-process) actor, or accessible across a network link.
pub struct Ref<M> {
    /// Mailbox of the actor owning the referenced entity.
    pub mailbox: Arc<Mailbox>,
    /// Mutex owning and guarding the state backing the referenced entity.
    pub target: Mutex<Option<Box<dyn Entity<M>>>>,
}

/// Specialization of `Ref<M>` for messages/assertions of type
/// [`AnyValue`].
///
/// All polyglot and network communication is done in terms of `Cap`s.
///
/// `Cap`s can also be *attenuated* ([Hardy 2017]; [Miller 2006]) to
/// reduce (or otherwise transform) the range of assertions and
/// messages they can be used to send to their referent. The
/// Syndicated Actor model uses
/// [Macaroon](https://syndicate-lang.org/doc/capabilities/)-style
/// capability attenuation.
///
/// [Hardy 2017]: http://cap-lore.com/CapTheory/Patterns/Attenuation.html
/// [Miller 2006]: http://www.erights.org/talks/thesis/markm-thesis.pdf
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cap {
    #[doc(hidden)]
    pub underlying: Arc<Ref<AnyValue>>,
    #[doc(hidden)]
    pub attenuation: Vec<CheckedCaveat>,
}

/// Adapter for converting an underlying [`Ref<M>`] to a [`Cap`].
///
/// The [`Entity`] implementation for `Guard` decodes `AnyValue`
/// assertions/messages to type `M` before passing them on to the
/// underlying entity.
pub struct Guard<M>
where
    for<'a> &'a M: Into<AnyValue>,
    for<'a> M: TryFrom<&'a AnyValue>,
{
    underlying: Arc<Ref<M>>
}

//---------------------------------------------------------------------------

const BUMP_AMOUNT: u8 = 10;

static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
#[doc(hidden)]
pub fn next_actor_id() -> ActorId {
    NEXT_ACTOR_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed)
}

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(3);
/// Allocate a process-unique `Handle`.
pub fn next_handle() -> Handle {
    NEXT_HANDLE.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed)
}

static NEXT_ACCOUNT_ID: AtomicU64 = AtomicU64::new(4);

preserves_schema::support::lazy_static! {
    #[doc(hidden)]
    pub static ref SYNDICATE_CREDIT: i64 = {
        let credit =
            std::env::var("SYNDICATE_CREDIT").unwrap_or("100".to_owned())
            .parse::<i64>().expect("Valid SYNDICATE_CREDIT environment variable");
        tracing::debug!("Configured SYNDICATE_CREDIT = {}", credit);
        credit
    };

    #[doc(hidden)]
    pub static ref ACCOUNTS: RwLock<Map<u64, (tracing::Span, Arc<AtomicI64>)>> =
        RwLock::new(Map::new());
}

impl TryFrom<&AnyValue> for Synced {
    type Error = ParseError;
    fn try_from(value: &AnyValue) -> Result<Self, Self::Error> {
        if let Some(true) = value.value().as_boolean() {
            Ok(Synced)
        } else {
            Err(ParseError::conformance_error("Synced"))
        }
    }
}

impl From<&Synced> for AnyValue {
    fn from(_value: &Synced) -> Self {
        AnyValue::new(true)
    }
}

impl<'activation> Activation<'activation> {
    fn make(actor: &ActorRef, account: Arc<Account>, state: &'activation mut RunningActor) -> Self {
        Activation {
            actor: actor.clone(),
            state,
            pending: EventBuffer::new(account),
        }
    }

    /// Constructs and executes `f` in a new "turn" for `actor`. If
    /// `f` returns `Ok(())`, [commits the turn][Self::deliver] and
    /// performs the buffered actions; otherwise, [abandons the
    /// turn][Self::clear] and discards the buffered actions.
    ///
    /// Bills any activity to `account`.
    pub fn for_actor<F>(
        actor: &ActorRef,
        account: Arc<Account>,
        f: F,
    ) -> ActorResult where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        match Self::for_actor_exit(actor, account, |t| match f(t) {
            Ok(()) => None,
            Err(e) => Some(Err(e)),
        }) {
            None => Ok(()),
            Some(e) => Err(error("Could not activate terminated actor", encode_error(e))),
        }
    }

    /// Constructs and executes `f` in a new "turn" for `actor`. If
    /// `f` returns `Some(exit_status)`, terminates `actor` with that
    /// `exit_status`. Otherwise, if `f` returns `None`, leaves
    /// `actor` in runnable state. [Commits buffered
    /// actions][Self::deliver] unless `actor` terminates with an
    /// `Err` status.
    ///
    /// Bills any activity to `account`.
    pub fn for_actor_exit<F>(
        actor: &ActorRef,
        account: Arc<Account>,
        f: F,
    ) -> Option<ActorResult> where
        F: FnOnce(&mut Activation) -> Option<ActorResult>,
    {
        match actor.state.lock() {
            Err(_) => panicked_err(),
            Ok(mut g) => match &mut *g {
                ActorState::Terminated { exit_status } =>
                    Some((**exit_status).clone()),
                ActorState::Running(state) => {
                    let mut activation = Activation::make(actor, account, state);
                    match f(&mut activation) {
                        None => None,
                        Some(exit_status) => {
                            if exit_status.is_err() {
                                activation.clear();
                            }
                            drop(activation);
                            let exit_status = Arc::new(exit_status);
                            let mut t = Activation::make(actor, Account::new(crate::name!("shutdown")), state);
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
                    }
                }
            }
        }
    }

    fn immediate_oid<M>(&self, r: &Arc<Ref<M>>) {
        if r.mailbox.actor_id != self.actor.actor_id {
            panic!("Cannot use for_myself to send to remote peers");
        }
    }

    /// Core API: assert `a` at recipient `r`.
    ///
    /// Returns the [`Handle`] for the new assertion.
    pub fn assert<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        let handle = next_handle();
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

    /// Core API: assert `a` at `r`, which must be a `Ref<M>` within the active actor.
    ///
    /// It's perfectly OK to use method [`assert`][Self::assert] even
    /// for `Ref`s that are part of the active actor. The difference
    /// between `assert` and `assert_for_myself` is that `r`'s handler
    /// for `assert` runs in a separate, later [`Activation`], while
    /// `r`'s handler for `assert_for_myself` runs in *this*
    /// [`Activation`], before it commits.
    ///
    /// Returns the [`Handle`] for the new assertion.
    ///
    /// # Panics
    ///
    /// Panics if `r` is not part of the active actor.
    pub fn assert_for_myself<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        self.immediate_oid(r);
        let handle = next_handle();
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

    /// Core API: retract a previously-established assertion.
    pub fn retract(&mut self, handle: Handle) {
        if let Some(d) = self.state.cleanup_actions.remove(&handle) {
            self.pending.execute_cleanup_action(d)
        }
    }

    /// Core API: send message `m` to recipient `r`.
    pub fn message<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, m: M) {
        let r = Arc::clone(r);
        self.pending.queue_for(&r).push(Box::new(
            move |t| r.with_entity(|e| e.message(t, m))))
    }

    /// Core API: send message `m` to recipient `r`, which must be a
    /// `Ref<M>` within the active actor.
    ///
    /// Method `message_for_myself` is to [`message`][Self::message]
    /// as [`assert_for_myself`][Self::assert_for_myself] is to
    /// [`assert`][Self::assert].
    ///
    /// # Panics
    ///
    /// Panics if `r` is not part of the active actor.
    pub fn message_for_myself<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, m: M) {
        self.immediate_oid(r);
        let r = Arc::clone(r);
        self.pending.for_myself.push(Box::new(
            move |t| r.with_entity(|e| e.message(t, m))))
    }

    /// Core API: begins a synchronisation with `r`.
    ///
    /// Once the synchronisation request reaches `r`'s actor, it will
    /// send a response to `peer`, which acts as a continuation for
    /// the synchronisation request.
    pub fn sync<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, peer: Arc<Ref<Synced>>) {
        let r = Arc::clone(r);
        self.pending.queue_for(&r).push(Box::new(
            move |t| r.with_entity(|e| e.sync(t, peer))))
    }

    /// Retrieve the [`Account`] against which actions are recorded.
    pub fn account(&self) -> &Arc<Account> {
        &self.pending.account
    }

    /// Discards all pending actions in this activation.
    pub fn clear(&mut self) {
        self.pending.clear();
    }

    /// Delivers all pending actions in this activation.
    ///
    /// This is called automatically when an `Activation` is
    /// `Drop`ped.
    ///
    /// # Panics
    ///
    /// Panics if any pending actions "`for_myself`" (resulting from
    /// [`assert_for_myself`][Self::assert_for_myself] or
    /// [`message_for_myself`][Self::message_for_myself]) are
    /// outstanding at the time of the call.
    pub fn deliver(&mut self) {
        self.pending.deliver();
    }
}

impl EventBuffer {
    fn new(account: Arc<Account>) -> Self {
        EventBuffer {
            account,
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

    fn clear(&mut self) {
        self.queues = HashMap::new();
        self.for_myself = PendingEventQueue::new();
    }

    fn deliver(&mut self) {
        if !self.for_myself.is_empty() {
            panic!("Unprocessed for_myself events remain at deliver() time");
        }
        for (_actor_id, (tx, turn)) in std::mem::take(&mut self.queues).into_iter() {
            let _ = send_actions(&tx, &self.account, turn);
        }
    }
}

impl Drop for EventBuffer {
    fn drop(&mut self) {
        self.deliver()
    }
}

impl Account {
    /// Construct a new `Account`, storing `name` within it for
    /// debugging use.
    pub fn new(name: tracing::Span) -> Arc<Self> {
        let id = NEXT_ACCOUNT_ID.fetch_add(1, Ordering::Relaxed);
        let debt = Arc::new(AtomicI64::new(0));
        ACCOUNTS.write().unwrap().insert(id, (name, Arc::clone(&debt)));
        Arc::new(Account {
            id,
            debt,
            notify: Notify::new(),
        })
    }

    /// Retrieve the current account balance: the number of
    /// currently-outstanding work items.
    pub fn balance(&self) -> i64 {
        self.debt.load(Ordering::Relaxed)
    }

    /// Borrow `token_count` work items against this account.
    pub fn borrow(&self, token_count: usize) {
        let token_count: i64 = token_count.try_into().expect("manageable token count");
        self.debt.fetch_add(token_count, Ordering::Relaxed);
    }

    /// Repay `token_count` work items previously borrowed against this account.
    pub fn repay(&self, token_count: usize) {
        let token_count: i64 = token_count.try_into().expect("manageable token count");
        let _old_debt = self.debt.fetch_sub(token_count, Ordering::Relaxed);
        if _old_debt - token_count <= *SYNDICATE_CREDIT {
            self.notify.notify_one();
        }
    }

    /// Suspend execution until enough "clear funds" exist in this
    /// account for some subsequent activity to be permissible.
    pub async fn ensure_clear_funds(&self) {
        let limit = *SYNDICATE_CREDIT;
        // tokio::task::yield_now().await;
        while self.balance() > limit {
            // tokio::task::yield_now().await;
            self.notify.notified().await;
        }
    }
}

impl Drop for Account {
    fn drop(&mut self) {
        ACCOUNTS.write().unwrap().remove(&self.id);
    }
}

impl<T> LoanedItem<T> {
    /// Construct a new `LoanedItem<T>` containing `item`, recording
    /// `cost` work items against `account`.
    pub fn new(account: &Arc<Account>, cost: usize, item: T) -> Self {
        account.borrow(cost);
        LoanedItem { account: Arc::clone(account), cost, item }
    }
}

impl<T> Drop for LoanedItem<T> {
    fn drop(&mut self) {
        self.account.repay(self.cost);
    }
}

#[must_use]
fn send_actions(
    tx: &UnboundedSender<SystemMessage>,
    account: &Arc<Account>,
    t: PendingEventQueue,
) -> ActorResult {
    let token_count = t.len();
    tx.send(SystemMessage::Turn(LoanedItem::new(account, token_count, t)))
        .map_err(|_| error("Target actor not running", AnyValue::new(false)))
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
    /// Create a new actor. It still needs to be
    /// [`start`ed][Self::start]/[`boot`ed][Self::boot].
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        let actor_id = next_actor_id();
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

    /// Create and start a new actor to own entity `e`. Returns a
    /// `Ref` to the new entity. The `name` is used as context for any
    /// log messages emitted by the new actor.
    pub fn create_and_start<M, E: Entity<M> + Send + 'static>(
        name: tracing::Span,
        e: E,
    ) -> Arc<Ref<M>> {
        let r = Self::create_and_start_inert(name);
        r.become_entity(e);
        r
    }

    /// Create and start a new actor, returning a `Ref` to a fresh
    /// entity contained within it. Before using the `Ref`, its
    /// initialization must be completed by calling
    /// [`become_entity`][Ref::become_entity] on it. The `name` is
    /// used as context for any log messages emitted by the new actor.
    pub fn create_and_start_inert<M>(name: tracing::Span) -> Arc<Ref<M>> {
        let ac = Self::new();
        let r = ac.ac_ref.access(|s| s.unwrap().expect_running().create_inert());
        ac.start(name);
        r
    }

    /// Start the actor's mainloop. Takes ownership of `self`. The
    /// `name` is used as context for any log messages emitted by the
    /// actor. The `boot` function is called in the actor's context,
    /// and then the mainloop is entered.
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

    /// Start the actor's mainloop. Takes ownership of `self`. The
    /// `name` is used as context for any log messages emitted by the
    /// actor. Delegates to [`boot`][Self::boot], with a no-op `boot`
    /// function.
    pub fn start(self, name: tracing::Span) -> ActorHandle {
        self.boot(name, |_ac| Ok(()))
    }

    fn terminate(&mut self, result: ActorResult) {
        let _ = Activation::for_actor_exit(
            &self.ac_ref, Account::new(crate::name!("shutdown")), |_| Some(result));
    }

    async fn run<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        boot: F,
    ) -> () {
        if Activation::for_actor(&self.ac_ref, Account::new(crate::name!("boot")), boot).is_err() {
            return;
        }

        loop {
            match self.rx.recv().await {
                None => {
                    return self.terminate(Err(error("Unexpected channel close", AnyValue::new(false))));
                }
                Some(m) => match m {
                    SystemMessage::Release => {
                        tracing::trace!("SystemMessage::Release");
                        return self.terminate(Ok(()));
                    }
                    SystemMessage::Turn(mut loaned_item) => {
                        let mut actions = std::mem::take(&mut loaned_item.item);
                        let r = Activation::for_actor(
                            &self.ac_ref, Arc::clone(&loaned_item.account), |t| {
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
    Some(Err(error("Actor panicked", AnyValue::new(false))))
}

impl ActorRef {
    /// Uses an internal mutex to access the internal state: takes the
    /// lock, calls `f` with the internal state, releases the lock,
    /// and returns the result of `f`.
    pub fn access<R, F: FnOnce(Option<&mut ActorState>) -> R>(&self, f: F) -> R {
        match self.state.lock() {
            Err(_) => f(None),
            Ok(mut g) => f(Some(&mut *g)),
        }
    }

    /// Retrieves the exit status of the denoted actor. If it is still
    /// running, yields `None`; otherwise, yields `Some(Ok(()))` if it
    /// exited normally, or `Some(Err(_))` if it terminated
    /// abnormally.
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
    /// Requests a shutdown of the actor. The shutdown request is
    /// handled by the actor's main loop, causing it to terminate with
    /// exit status `Ok(())`.
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

    /// Construct an entity with behaviour [`InertEntity`] within this
    /// actor.
    pub fn inert_entity<M>(&mut self) -> Arc<Ref<M>> {
        self.create(InertEntity)
    }

    /// Construct an entity with behaviour `e` within this actor.
    pub fn create<M, E: Entity<M> + Send + 'static>(&mut self, e: E) -> Arc<Ref<M>> {
        let r = self.create_inert();
        r.become_entity(e);
        r
    }

    /// Construct an entity whose behaviour will be specified later
    /// (via [`become_entity`][Ref::become_entity]).
    pub fn create_inert<M>(&mut self) -> Arc<Ref<M>> {
        Arc::new(Ref {
            mailbox: self.mailbox(),
            target: Mutex::new(None),
        })
    }

    /// Registers the entity `r` in the list of exit hooks for this
    /// actor. When the actor terminates, `r`'s
    /// [`exit_hook`][Entity::exit_hook] will be called.
    pub fn add_exit_hook<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>) {
        let r = Arc::clone(r);
        self.exit_hooks.push(Box::new(move |t, exit_status| {
            r.with_entity(|e| e.exit_hook(t, &exit_status))
        }))
    }

    /// Start a new [linked task][crate::actor#linked-tasks] attached
    /// to this actor. The function `boot` is the main function of the
    /// new task. Uses `name` for log messages emitted by the task.
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
            let mut b = EventBuffer::new(Account::new(crate::name!("drop")));
            for (_handle, r) in to_clear.into_iter() {
                tracing::trace!(h = debug(&_handle), "retract on termination");
                b.execute_cleanup_action(r);
            }
        }

        tracing::trace!("Actor::drop");
    }
}

/// Directly injects `action` into `mailbox`, billing subsequent activity against `account`.
///
/// Primarily for use by [linked tasks][RunningActor::linked_task].
#[must_use]
pub fn external_event(mailbox: &Arc<Mailbox>, account: &Arc<Account>, action: Action) -> ActorResult {
    send_actions(&mailbox.tx, account, vec![action])
}

/// Directly injects `actions` into `mailbox`, billing subsequent activity against `account`.
///
/// Primarily for use by [linked tasks][RunningActor::linked_task].
#[must_use]
pub fn external_events(mailbox: &Arc<Mailbox>, account: &Arc<Account>, actions: PendingEventQueue) -> ActorResult {
    send_actions(&mailbox.tx, account, actions)
}

impl<M> Ref<M> {
    /// Supplies the behaviour (`e`) for a `Ref` created via
    /// [`create_inert`][RunningActor::create_inert].
    ///
    /// # Panics
    ///
    /// Panics if this `Ref` has already been given a behaviour.
    pub fn become_entity<E: 'static + Entity<M>>(&self, e: E) {
        let mut g = self.target.lock().expect("unpoisoned");
        if g.is_some() {
            panic!("Double initialization of Ref");
        }
        *g = Some(Box::new(e));
    }

    #[doc(hidden)]
    pub fn with_entity<R, F: FnOnce(&mut dyn Entity<M>) -> R>(&self, f: F) -> R {
        let mut g = self.target.lock().expect("unpoisoned");
        f(g.as_mut().expect("initialized").as_mut())
    }
}

impl<M> Ref<M> {
    /// Retrieves a process-unique identifier for the `Ref`; `Ref`s
    /// are compared by this identifier.
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
    /// Given a `Ref<M>`, where `M` is interconvertible with
    /// `AnyValue`, yields a `Cap` for the referenced entity. The
    /// `Cap` automatically decodes presented `AnyValue`s into
    /// instances of `M`.
    pub fn guard<M: 'static + Send>(underlying: &Arc<Ref<M>>) -> Arc<Self>
    where
        for<'a> &'a M: Into<AnyValue>,
        for<'a> M: TryFrom<&'a AnyValue>,
    {
        Self::new(&Arc::new(Ref {
            mailbox: Arc::clone(&underlying.mailbox),
            target: Mutex::new(Some(Box::new(Guard { underlying: underlying.clone() }))),
        }))
    }

    /// Directly constructs a `Cap` for `underlying`.
    pub fn new(underlying: &Arc<Ref<AnyValue>>) -> Arc<Self> {
        Arc::new(Cap {
            underlying: Arc::clone(underlying),
            attenuation: Vec::new(),
        })
    }

    /// Yields a fresh `Cap` for `self`'s `underlying`, copying the
    /// existing attenuation of `self` to the new `Cap` and adding
    /// `attenuation` to it.
    pub fn attenuate(&self, attenuation: &sturdy::Attenuation) -> Result<Arc<Self>, CaveatError> {
        let mut r = Cap { attenuation: self.attenuation.clone(), .. self.clone() };
        r.attenuation.extend(attenuation.check()?);
        Ok(Arc::new(r))
    }

    /// Applies the contained attenuation to `a`, yielding `None` if
    /// `a` is filtered out, or `Some(_)` if it is accepted (and
    /// possibly transformed).
    pub fn rewrite(&self, mut a: AnyValue) -> Option<AnyValue> {
        for c in &self.attenuation {
            match c.rewrite(&a) {
                Some(v) => a = v,
                None => return None,
            }
        }
        Some(a)
    }

    /// Translates `m` into an `AnyValue`, passes it through
    /// [`rewrite`][Self::rewrite], and then
    /// [`assert`s][Activation::assert] it using the activation `t`.
    pub fn assert<M: Into<AnyValue>>(&self, t: &mut Activation, m: M) -> Option<Handle> {
        self.rewrite(m.into()).map(|m| t.assert(&self.underlying, m))
    }

    /// Translates `m` into an `AnyValue`, passes it through
    /// [`rewrite`][Self::rewrite], and then sends it via method
    /// [`message`][Activation::message] on the activation `t`.
    pub fn message<M: Into<AnyValue>>(&self, t: &mut Activation, m: M) {
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

impl<M> Entity<AnyValue> for Guard<M>
where
    for<'a> &'a M: Into<AnyValue>,
    for<'a> M: TryFrom<&'a AnyValue>,
{
    fn assert(&mut self, t: &mut Activation, a: AnyValue, h: Handle) -> ActorResult {
        match M::try_from(&a) {
            Ok(a) => self.underlying.with_entity(|e| e.assert(t, a, h)),
            Err(_) => Ok(()),
        }
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        self.underlying.with_entity(|e| e.retract(t, h))
    }
    fn message(&mut self, t: &mut Activation, m: AnyValue) -> ActorResult {
        match M::try_from(&m) {
            Ok(m) => self.underlying.with_entity(|e| e.message(t, m)),
            Err(_) => Ok(()),
        }
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        self.underlying.with_entity(|e| e.sync(t, peer))
    }
    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        self.underlying.with_entity(|e| e.exit_hook(t, exit_status))
    }
}

/// A convenient Syndicate-enhanced variation on
/// [`tracing::info_span`].
///
/// Includes fields `actor_id`, `task_id` and `oid`, so that they show
/// up in those circumstances where they happen to be defined as part
/// of the operation of the [`crate::actor`] module.
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
