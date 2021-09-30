#![doc = concat!(
    include_str!("../doc/actor.md"),
    include_str!("../doc/what-is-an-actor.md"),
    include_str!("../doc/flow-control.md"),
    include_str!("../doc/linked-tasks.md"),
)]

use super::dataflow::Graph;
use super::error::Error;
use super::error::encode_error;
use super::error::error;
use super::rewrite::CaveatError;
use super::rewrite::CheckedCaveat;
use super::schemas::sturdy;

use parking_lot::Mutex;
use parking_lot::RwLock;

use preserves::value::ArcValue;
use preserves::value::Domain;
use preserves::value::IOValue;
use preserves::value::Map;
use preserves::value::NestedValue;
use preserves::value::Set;
use preserves_schema::ParseError;
use preserves_schema::support::Parse;
use preserves_schema::support::Unparse;

use std::any::Any;
use std::boxed::Box;
use std::collections::hash_map::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time;

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
pub type AnyValue = ArcValue<Arc<Cap>>;

/// The type of process-unique actor IDs.
pub type ActorId = u64;

/// The type of process-unique facet IDs.
pub type FacetId = NonZeroU64;

/// The type of process-unique assertion handles.
///
/// Used both as a reference to [retract][Entity::retract]
/// previously-asserted assertions and as an indexing key to associate
/// local state with some incoming assertion in an entity.
pub type Handle = u64;

/// The type of process-unique field instance IDs.
pub type FieldId = NonZeroU64;

/// The type of process-unique field observer block IDs.
pub type BlockId = NonZeroU64;

/// Responses to events must have type `ActorResult`.
pub type ActorResult = Result<(), Error>;

/// The [`Actor::boot`] method returns an `ActorHandle`, representing
/// the actor's mainloop task.
pub type ActorHandle = tokio::task::JoinHandle<ActorResult>;

/// The type of the "disarm" function returned from [`Activation::prevent_inert_check`].
pub type DisarmFn = Box<dyn Send + FnOnce()>;

/// A small protocol for indicating successful synchronisation with
/// some peer; see [Entity::sync].
#[derive(Debug)]
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

    /// Optional callback for running actions when the entity's owning [Facet] terminates
    /// cleanly. Will not be called in case of abnormal shutdown (crash) of an actor.
    ///
    /// Programs register an entity's stop hook with [Activation::on_stop_notify].
    ///
    /// The default implementation does nothing.
    fn stop(&mut self, turn: &mut Activation) -> ActorResult {
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
type Block = Box<dyn Send + FnMut(&mut Activation) -> ActorResult>;

#[doc(hidden)]
pub type PendingEventQueue = Vec<Action>;

/// The main API for programming Syndicated Actor objects.
///
/// Through `Activation`s, programs can access the state of their
/// animating [`RunningActor`] and their active [`Facet`].
///
/// Usually, an `Activation` will be supplied to code that needs one; but when non-Actor code
/// (such as a [linked task][crate::actor#linked-tasks]) needs to enter an Actor's execution
/// context, use [`FacetRef::activate`] to construct one.
///
/// Many actions that an entity can perform are methods directly on
/// `Activation`, but methods on the [`RunningActor`] and [`FacetRef`]
/// values contained in an `Activation` are also sometimes useful.
///
/// This is what other implementations call a "Turn", renamed here to
/// avoid conflicts with [`crate::schemas::protocol::Turn`].
pub struct Activation<'activation> {
    /// A reference to the currently active [`Facet`] and the implementation-side state of its
    /// [`Actor`].
    pub facet: FacetRef,
    /// A reference to the current state of the active [`Actor`].
    pub state: &'activation mut RunningActor,
    active_block: Option<BlockId>,
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
    ReleaseField(FieldId),
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

/// A combination of an [`ActorRef`] with a [`FacetId`], acting as a capability to enter the
/// execution context of a facet from a linked task.
#[derive(Clone)]
pub struct FacetRef {
    pub actor: ActorRef,
    pub facet_id: FacetId,
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
    dataflow: Graph<FieldId, BlockId>,
    fields: HashMap<FieldId, Box<dyn Any + Send>>,
    blocks: HashMap<BlockId, (FacetId, Block)>,
    exit_hooks: Vec<Box<dyn Send + FnOnce(&mut Activation, &Arc<ActorResult>) -> ActorResult>>,
    cleanup_actions: CleanupActions,
    facet_nodes: Map<FacetId, Facet>,
    facet_children: Map<FacetId, Set<FacetId>>,
    root: FacetId,
}

/// Handle to a shared, mutable field (i.e. a *dataflow variable*) within a [`RunningActor`].
///
/// Use [`Activation::field`] to create fields, and use [`Activation::get`],
/// [`::get_mut`][Activation::get_mut], and [`::set`][Activation::set] to read and write field
/// values. Use [`Activation::dataflow`] to create a reactive block within a facet that will be
/// (re-)executed whenever some dependent field changes value.
///
pub struct Field<T: Any + Send> {
    pub name: String,
    pub field_id: FieldId,
    tx: UnboundedSender<SystemMessage>,
    phantom: PhantomData<T>,
}

/// State associated with each facet in an [`Actor`]'s facet tree.
///
/// # Inert facets
///
/// A facet is considered *inert* if:
///
/// 1. it has no child facets;
/// 2. it has no cleanup actions (that is, no assertions placed by any of its entities);
/// 3. it has no linked tasks; and
/// 4. it has no "inert check preventers" (see [Activation::prevent_inert_check]).
///
/// If a facet is created and is inert at the moment that its `boot` function returns, it is
/// automatically terminated.
///
/// When a facet is terminated, if its parent facet is inert, the parent is terminated.
///
/// If the root facet in an actor is terminated, the entire actor is terminated (with exit
/// status `Ok(())`).
///
pub struct Facet {
    /// The ID of the facet.
    pub facet_id: FacetId,
    /// The ID of the facet's parent facet, if any; if None, this facet is the `Actor`'s root facet.
    pub parent_facet_id: Option<FacetId>,
    outbound_handles: Set<Handle>,
    stop_actions: Vec<Action>,
    linked_tasks: Map<u64, CancellationToken>,
    inert_check_preventers: Arc<AtomicU64>,
}

/// A reference to an object that expects messages/assertions of type
/// `M`.
///
/// The object can be in the same actor, in a different local
/// (in-process) actor, or accessible across a network link.
pub struct Ref<M> {
    /// Mailbox of the actor owning the referenced entity.
    pub mailbox: Arc<Mailbox>,
    /// ID of the facet (within the actor) owning the referenced entity.
    pub facet_id: FacetId,
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
pub struct Guard<L, M>
where
    M: for<'a> Unparse<&'a L, AnyValue>,
    M: for<'a> Parse<&'a L, AnyValue>,
{
    underlying: Arc<Ref<M>>,
    literals: Arc<L>,
}

/// Simple entity that stops its containing facet when any assertion it receives is
/// subsequently retracted.
pub struct StopOnRetract;

/// Returned from the function given to [`FacetRef::activate_exit`] to indicate how the actor
/// should proceed.
pub enum RunDisposition {
    Continue,
    Terminate(ActorResult),
}

/// [Linked tasks][Activation::linked_task] terminate yielding values of this type.
pub enum LinkedTaskTermination {
    /// Causes the task's associated [Facet] to be [stop][Activation::stop]ped when the task
    /// returns.
    Normal,
    /// Causes no action to be taken regarding the task's associated [Facet] at the time the
    /// task returns.
    KeepFacet,
}

//---------------------------------------------------------------------------

const BUMP_AMOUNT: u8 = 10;

static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
#[doc(hidden)]
pub fn next_actor_id() -> ActorId {
    NEXT_ACTOR_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed)
}

static NEXT_FACET_ID: AtomicU64 = AtomicU64::new(2);
#[doc(hidden)]
pub fn next_facet_id() -> FacetId {
    FacetId::new(NEXT_FACET_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed))
        .expect("Internal error: Attempt to allocate FacetId of zero. Too many FacetIds allocated. Restart the process.")
}

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(3);
/// Allocate a process-unique `Handle`.
pub fn next_handle() -> Handle {
    NEXT_HANDLE.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed)
}

static NEXT_ACCOUNT_ID: AtomicU64 = AtomicU64::new(4);

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(5);

static NEXT_FIELD_ID: AtomicU64 = AtomicU64::new(6);

static NEXT_BLOCK_ID: AtomicU64 = AtomicU64::new(7);

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

impl<'a> Parse<&'a (), AnyValue> for Synced {
    fn parse(_language: &'a (), value: &AnyValue) -> Result<Self, ParseError> {
        Synced::try_from(value)
    }
}

impl<'a> Unparse<&'a (), AnyValue> for Synced {
    fn unparse(&self, _language: &'a ()) -> AnyValue {
        self.into()
    }
}

impl From<ActorResult> for RunDisposition {
    fn from(v: ActorResult) -> Self {
        match v {
            Ok(()) => RunDisposition::Continue,
            Err(e) => RunDisposition::Terminate(Err(e)),
        }
    }
}

impl FacetRef {
    /// Executes `f` in a new "[turn][Activation]" for `actor`. If `f` returns `Ok(())`,
    /// [commits the turn][Activation::deliver] and performs the buffered actions; otherwise,
    /// [abandons the turn][Activation::clear] and discards the buffered actions.
    ///
    /// Bills any activity to `account`.
    pub fn activate<F>(
        &self,
        account: Arc<Account>,
        f: F,
    ) -> ActorResult where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        self.activate_exit(account, |t| f(t).into())
    }

    /// Executes `f` in a new "[turn][Activation]" for `actor`. If `f` returns
    /// `Some(exit_status)`, terminates `actor` with that `exit_status`. Otherwise, if `f`
    /// returns `None`, leaves `actor` in runnable state. [Commits buffered
    /// actions][Activation::deliver] unless `actor` terminates with an `Err` status.
    ///
    /// Bills any activity to `account`.
    pub fn activate_exit<F>(
        &self,
        account: Arc<Account>,
        f: F,
    ) -> ActorResult where
        F: FnOnce(&mut Activation) -> RunDisposition,
    {
        let mut g = self.actor.state.lock();
        match &mut *g {
            ActorState::Terminated { exit_status } =>
                Err(error("Could not activate terminated actor",
                          encode_error((**exit_status).clone()))),
            ActorState::Running(state) => {
                tracing::trace!(actor_id=?self.actor.actor_id, "activate");
                let mut activation = Activation::make(self, account, state);
                let f_result = f(&mut activation);
                let result = match activation.restore_invariants(f_result) {
                    RunDisposition::Continue => Ok(()),
                    RunDisposition::Terminate(exit_status) => {
                        if exit_status.is_err() {
                            activation.clear();
                        }
                        drop(activation);
                        let exit_status = Arc::new(exit_status);
                        let mut t = Activation::make(&self.actor.facet_ref(state.root),
                                                     Account::new(crate::name!("shutdown")),
                                                     state);
                        if let Err(err) = t._terminate_facet(t.state.root, exit_status.is_ok()) {
                            // This can only occur as the result of an internal error in this file's code.
                            tracing::error!(?err, "unexpected error from terminate_facet");
                            panic!("Unexpected error result from terminate_facet");
                        }
                        // TODO: The linked_tasks are being cancelled above ^ when their Facets drop.
                        // TODO: We don't want that: we want (? do we?) exit hooks to run before linked_tasks are cancelled.
                        // TODO: Example: send an error message in an exit_hook that is processed and delivered by a linked_task.
                        for action in std::mem::take(&mut t.state.exit_hooks) {
                            if let Err(err) = action(&mut t, &exit_status) {
                                tracing::error!(?err, "error in exit hook");
                            }
                        }
                        *g = ActorState::Terminated {
                            exit_status: Arc::clone(&exit_status),
                        };
                        (*exit_status).clone()
                    }
                };
                tracing::trace!(actor_id=?self.actor.actor_id, "deactivate");
                result
            }
        }
    }
}

impl<'activation> Activation<'activation> {
    fn make(
        facet: &FacetRef,
        account: Arc<Account>,
        state: &'activation mut RunningActor,
    ) -> Self {
        Activation {
            facet: facet.clone(),
            state,
            active_block: None,
            pending: EventBuffer::new(account),
        }
    }

    fn immediate_oid<M>(&self, r: &Arc<Ref<M>>) {
        if r.mailbox.actor_id != self.facet.actor.actor_id {
            panic!("Cannot use for_myself to send to remote peers");
        }
    }

    fn with_facet<F>(&mut self, check_existence: bool, facet_id: FacetId, f: F) -> ActorResult
    where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        if !check_existence || self.state.facet_nodes.contains_key(&facet_id) {
            tracing::trace!(check_existence, facet_id, "is_alive");
            let old_facet_id = self.facet.facet_id;
            self.facet.facet_id = facet_id;
            let result = f(self);
            self.facet.facet_id = old_facet_id;
            result
        } else {
            tracing::trace!(facet_id, "not_alive");
            Ok(())
        }
    }

    #[doc(hidden)]
    pub fn with_entity<M, F>(&mut self, r: &Arc<Ref<M>>, f: F) -> ActorResult where
        F: FnOnce(&mut Activation, &mut dyn Entity<M>) -> ActorResult
    {
        self.with_facet(true, r.facet_id, |t| r.internal_with_entity(|e| f(t, e)))
    }

    fn active_facet<'a>(&'a mut self) -> Option<&'a mut Facet> {
        self.state.get_facet(self.facet.facet_id)
    }

    /// Core API: assert `a` at recipient `r`.
    ///
    /// Returns the [`Handle`] for the new assertion.
    pub fn assert<M: 'static + Send + std::fmt::Debug>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        let handle = next_handle();
        if let Some(f) = self.state.get_facet(self.facet.facet_id) {
            tracing::trace!(?r, ?handle, ?a, "assert");
            f.outbound_handles.insert(handle);
            drop(f);
            self.state.insert_retract_cleanup_action(&r, handle);
            {
                let r = Arc::clone(r);
                self.pending.queue_for(&r).push(Box::new(
                    move |t| t.with_entity(&r, |t, e| {
                        tracing::trace!(?handle, ?a, "asserted");
                        e.assert(t, a, handle)
                    })));
            }
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
    pub fn assert_for_myself<M: 'static + Send + std::fmt::Debug>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        self.immediate_oid(r);
        let handle = next_handle();
        if let Some(f) = self.active_facet() {
            tracing::trace!(?r, ?handle, ?a, "assert_for_myself");
            f.outbound_handles.insert(handle);
            drop(f);
            {
                let r = Arc::clone(r);
                self.state.cleanup_actions.insert(
                    handle,
                    CleanupAction::ForMyself(Box::new(
                        move |t| t.with_entity(&r, |t, e| {
                            tracing::trace!(?handle, "retracted");
                            if let Some(f) = t.active_facet() {
                                f.outbound_handles.remove(&handle);
                            }
                            e.retract(t, handle)
                        }))));
            }
            {
                let r = Arc::clone(r);
                self.pending.for_myself.push(Box::new(
                    move |t| t.with_entity(&r, |t, e| {
                        tracing::trace!(?handle, ?a, "asserted");
                        e.assert(t, a, handle)
                    })));
            }
        }
        handle
    }

    fn half_link(&mut self, t_other: &mut Activation) {
        let entity_ref = t_other.create::<AnyValue, _>(StopOnRetract);
        let handle = next_handle();
        tracing::trace!(?handle, ?entity_ref, "half_link");
        self.state.insert_retract_cleanup_action(&entity_ref, handle);
        self.active_facet().unwrap().outbound_handles.insert(handle);
        t_other.with_entity(&entity_ref, |t, e| e.assert(t, AnyValue::new(true), handle)).unwrap();
    }

    /// Core API: retract a previously-established assertion.
    pub fn retract(&mut self, handle: Handle) {
        if let Some(d) = self.state.cleanup_actions.remove(&handle) {
            self.pending.execute_cleanup_action(d)
        }
    }

    /// Core API: assert, retract, or replace an assertion.
    pub fn update<M: 'static + Send + std::fmt::Debug>(
        &mut self,
        handle: &mut Option<Handle>,
        r: &Arc<Ref<M>>,
        a: Option<M>,
    ) {
        let saved = handle.take();
        if let Some(a) = a {
            *handle = Some(self.assert(r, a));
        }
        if let Some(h) = saved {
            self.retract(h);
        }
    }

    /// Core API: send message `m` to recipient `r`.
    pub fn message<M: 'static + Send + std::fmt::Debug>(&mut self, r: &Arc<Ref<M>>, m: M) {
        tracing::trace!(?r, ?m, "message");
        let r = Arc::clone(r);
        self.pending.queue_for(&r).push(Box::new(
            move |t| t.with_entity(&r, |t, e| {
                tracing::trace!(?m, "delivered");
                e.message(t, m)
            })))
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
            move |t| t.with_entity(&r, |t, e| e.message(t, m))))
    }

    /// Core API: begins a synchronisation with `r`.
    ///
    /// Once the synchronisation request reaches `r`'s actor, it will
    /// send a response to `peer`, which acts as a continuation for
    /// the synchronisation request.
    pub fn sync<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, peer: Arc<Ref<Synced>>) {
        let r = Arc::clone(r);
        self.pending.queue_for(&r).push(Box::new(
            move |t| t.with_entity(&r, |t, e| e.sync(t, peer))))
    }

    /// Registers the entity `r` in the list of stop actions for the active facet. If the facet
    /// terminates cleanly, `r`'s [`stop`][Entity::stop] will be called.
    ///
    /// **Note.** If the actor crashes, stop actions will *not* be called.
    ///
    /// Use [`RunningActor::add_exit_hook`] to install a callback that will be called at the
    /// end of the lifetime of the *actor* rather than the facet. (Also, exit hooks are called
    /// no matter whether actor termination was normal or abnormal.)
    pub fn on_stop_notify<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>) {
        if let Some(f) = self.active_facet() {
            let r = Arc::clone(r);
            f.stop_actions.push(Box::new(move |t| r.internal_with_entity(|e| e.stop(t))));
        }
    }

    /// Registers `action` in the list of stop actions for the active facet. If the facet
    /// terminates cleanly, `action` will be called. See also notes against
    /// [`on_stop_notify`][Activation::on_stop_notify].
    pub fn on_stop<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(&mut self, action: F) {
        if let Some(f) = self.active_facet() {
            f.stop_actions.push(Box::new(action));
        }
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

    /// Construct an entity with behaviour [`InertEntity`] within the active facet.
    pub fn inert_entity<M>(&mut self) -> Arc<Ref<M>> {
        self.create(InertEntity)
    }

    /// Construct an entity with behaviour `e` within the active facet.
    pub fn create<M, E: Entity<M> + Send + 'static>(&mut self, e: E) -> Arc<Ref<M>> {
        let r = self.create_inert();
        r.become_entity(e);
        r
    }

    /// Construct an entity (within the active facet) whose behaviour will be specified later
    /// via [`become_entity`][Ref::become_entity].
    pub fn create_inert<M>(&mut self) -> Arc<Ref<M>> {
        Arc::new(Ref {
            mailbox: self.state.mailbox(),
            facet_id: self.facet.facet_id,
            target: Mutex::new(None),
        })
    }

    /// Start a new [linked task][crate::actor#linked-tasks] attached to the active facet. The
    /// task will execute the future "`boot`" to completion unless it is cancelled first (by
    /// e.g. termination of the owning facet or crashing of the owning actor). Stops the active
    /// facet when the linked task completes. Uses `name` for log messages emitted by the task.
    pub fn linked_task<F: 'static + Send + futures::Future<Output = Result<LinkedTaskTermination, Error>>>(
        &mut self,
        name: tracing::Span,
        boot: F,
    ) {
        let mailbox = self.state.mailbox();
        let facet = self.facet.clone();
        if let Some(f) = self.active_facet() {
            let token = CancellationToken::new();
            let task_id = NEXT_TASK_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed);
            name.record("task_id", &task_id);
            {
                let token = token.clone();
                tokio::spawn(async move {
                    tracing::trace!(task_id, "linked task start");
                    let result = select! {
                        _ = token.cancelled() => {
                            tracing::trace!(task_id, "linked task cancelled");
                            LinkedTaskTermination::Normal
                        }
                        result = boot => match result {
                            Ok(t) => {
                                tracing::trace!(task_id, "linked task normal stop");
                                t
                            }
                            Err(e) => {
                                tracing::error!(task_id, "linked task error: {}", e);
                                let _ = mailbox.tx.send(SystemMessage::Crash(e.clone()));
                                Err(e)?
                            }
                        }
                    };
                    let _ = facet.activate(
                        Account::new(crate::name!("release_linked_task")), |t| {
                            if let Some(f) = t.active_facet() {
                                tracing::trace!(task_id, "cancellation token removed");
                                f.linked_tasks.remove(&task_id);
                            }
                            if let LinkedTaskTermination::Normal = result {
                                t.stop()?;
                            }
                            Ok(())
                        });
                    Ok::<(), Error>(())
                }.instrument(name));
            }
            f.linked_tasks.insert(task_id, token);
        }
    }

    /// Executes the given action after the given duration has elapsed (so long as the active
    /// facet still exists at that time).
    pub fn after<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        duration: time::Duration,
        a: F,
    ) {
        self.at(time::Instant::now() + duration, a)
    }

    /// Executes the given action at the given instant (so long as the active facet still
    /// exists at that time).
    pub fn at<I: Into<tokio::time::Instant>, F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        instant: I,
        a: F,
    ) {
        let account = Arc::clone(self.account());
        let instant = instant.into();
        let facet = self.facet.clone();
        let span = tracing::Span::current().clone();
        self.linked_task(crate::name!(parent: None, "Activation::at"), async move {
            tokio::time::sleep_until(instant.into()).await;
            let _entry = span.enter();
            facet.activate(account, a)?;
            Ok(LinkedTaskTermination::KeepFacet)
        });
    }

    fn enqueue_for_myself_at_commit(&mut self, action: Action) {
        let mailbox = self.state.mailbox();
        self.pending.queue_for_mailbox(&mailbox).push(action);
    }

    /// Schedule the creation of a new actor when the Activation commits.
    pub fn spawn<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorRef {
        let ac = Actor::new();
        let ac_ref = ac.ac_ref.clone();
        self.enqueue_for_myself_at_commit(Box::new(move |_| {
            ac.boot(name, boot);
            Ok(())
        }));
        ac_ref
    }

    /// Schedule the creation of a new actor when the Activation commits.
    ///
    /// The new actor will be "linked" to the active facet: if the new actor terminates, the
    /// active facet is stopped, and if the active facet stops, the new actor's root facet is
    /// stopped.
    pub fn spawn_link<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        name: tracing::Span,
        boot: F,
    ) -> ActorRef {
        let ac = Actor::new();
        let ac_ref = ac.ac_ref.clone();
        let facet_id = self.facet.facet_id;
        self.enqueue_for_myself_at_commit(Box::new(move |t| {
            t.with_facet(true, facet_id, move |t| {
                ac.link(t).boot(name, boot);
                Ok(())
            })
        }));
        ac_ref
    }

    /// Create a new subfacet of the currently-active facet. Runs `boot` in the new facet's
    /// context. If `boot` returns leaving the new facet [inert][Facet#inert-facets],
    pub fn facet<F: FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        boot: F,
    ) -> Result<FacetId, Error> {
        let f = Facet::new(Some(self.facet.facet_id));
        let facet_id = f.facet_id;
        self.state.facet_nodes.insert(facet_id, f);
        tracing::debug!(parent_id = ?self.facet.facet_id,
                        ?facet_id,
                        actor_facet_count = ?self.state.facet_nodes.len());
        self.state.facet_children.entry(self.facet.facet_id).or_default().insert(facet_id);
        self.with_facet(true /* TODO: tiny optimisation: "false" would be safe here */, facet_id, move |t| {
            boot(t)?;
            t.stop_if_inert();
            Ok(())
        })?;
        Ok(facet_id)
    }

    /// Useful during facet (and actor) startup, in some situations: when a facet `boot`
    /// procedure would return while the facet is inert, but the facet should survive until
    /// some subsequent time, call `prevent_inert_check` to increment a counter that prevents
    /// inertness-checks from succeeding on the active facet.
    ///
    /// The result of `prevent_inert_check` is a function which, when called, decrements the
    /// counter again. After the counter has been decremented, any subsequent inertness checks
    /// will no longer be artificially forced to fail.
    ///
    /// An example of when you might want this: creating an actor having only a single
    /// Dataspace entity within it, then using the Dataspace from other actors. At the start of
    /// its life, the Dataspace actor will have no outbound assertions, no child facets, and no
    /// linked tasks, so the only way to prevent it from being prematurely garbage collected is
    /// to use `prevent_inert_check` in its boot function.
    pub fn prevent_inert_check(&mut self) -> DisarmFn {
        if let Some(f) = self.active_facet() {
            Box::new(f.prevent_inert_check())
        } else {
            Box::new(|| ())
        }
    }

    /// Arranges for the [`Facet`] named by `facet_id` to be stopped cleanly when `self`
    /// commits.
    ///
    /// Then,
    ///  - if `continuation` is supplied, and
    ///  - the facet to be stopped hasn't been stopped at the time of the `stop_facet_and_continue` call, and
    ///  - none of the shutdown handlers yields an error, and
    ///  - the facet's parent facet is alive,
    /// executes `continuation` (immediately) in the *parent* facet's context.
    ///
    pub fn stop_facet_and_continue<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        facet_id: FacetId,
        continuation: Option<F>,
    ) -> ActorResult {
        let maybe_parent_id = self.active_facet().and_then(|f| f.parent_facet_id);
        self.enqueue_for_myself_at_commit(Box::new(move |t| t._terminate_facet(facet_id, true)));
        if let Some(k) = continuation {
            if let Some(parent_id) = maybe_parent_id {
                self.with_facet(true, parent_id, k)?;
            }
        }
        Ok(())
    }

    /// Arranges for the [`Facet`] named by `facet_id` to be stopped cleanly when `self`
    /// commits.
    ///
    /// Equivalent to `self.stop_facet_and_continue(facet_id, None)`.
    pub fn stop_facet(&mut self, facet_id: FacetId) -> ActorResult {
        self.stop_facet_and_continue::<Action>(facet_id, None)
    }

    /// Arranges for the active facet to be stopped cleanly when `self` commits.
    ///
    /// Equivalent to `self.stop_facet(self.facet.facet_id)`.
    pub fn stop(&mut self) -> ActorResult {
        self.stop_facet(self.facet.facet_id)
    }

    fn stop_if_inert(&mut self) {
        let facet_id = self.facet.facet_id;
        self.enqueue_for_myself_at_commit(Box::new(move |t| {
            tracing::trace!("Checking inertness of facet {} from facet {}", facet_id, t.facet.facet_id);
            if t.state.facet_exists_and_is_inert(facet_id) {
                tracing::trace!(" - facet {} is inert, stopping it", facet_id);
                t.stop_facet(facet_id)?;
            } else {
                tracing::trace!(" - facet {} is not inert", facet_id);
            }
            Ok(())
        }))
    }

    fn _terminate_facet(&mut self, facet_id: FacetId, alive: bool) -> ActorResult {
        if let Some(mut f) = self.state.facet_nodes.remove(&facet_id) {
            tracing::debug!(actor_facet_count = ?self.state.facet_nodes.len(),
                            "{} termination of {:?}",
                            if alive { "living" } else { "post-exit" },
                            facet_id);
            if let Some(p) = f.parent_facet_id {
                self.state.facet_children.get_mut(&p).map(|children| children.remove(&facet_id));
            }
            self.with_facet(false, facet_id, |t| {
                if let Some(children) = t.state.facet_children.remove(&facet_id) {
                    for child_id in children.into_iter() {
                        t._terminate_facet(child_id, alive)?;
                    }
                }
                if alive {
                    for action in std::mem::take(&mut f.stop_actions) {
                        action(t)?;
                    }
                    let parent_facet_id = f.parent_facet_id;
                    f.retract_outbound(t);
                    // ^ we need retraction to happen right here so that child-facet
                    // cleanup-actions are performed before parent-facet cleanup-actions.
                    if let Some(p) = parent_facet_id {
                        if t.state.facet_exists_and_is_inert(p) {
                            tracing::trace!("terminating parent {:?} of facet {:?}", p, facet_id);
                            t._terminate_facet(p, true)?;
                        } else {
                            tracing::trace!("not terminating parent {:?} of facet {:?}", p, facet_id);
                        }
                    } else {
                        tracing::trace!("terminating actor of root facet {:?}", facet_id);
                        t.state.shutdown();
                    }
                } else {
                    f.retract_outbound(t);
                }
                Ok(())
            })
        } else {
            Ok(())
        }
    }

    /// Create a new named dataflow variable (field) within the active [`Actor`].
    pub fn named_field<T: Any + Send>(&mut self, name: &str, initial_value: T) -> Arc<Field<T>> {
        let field_id = FieldId::new(NEXT_FIELD_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed))
            .expect("Internal error: Attempt to allocate FieldId of zero. Too many FieldIds allocated. Restart the process.");
        self.state.fields.insert(field_id, Box::new(initial_value));
        Arc::new(Field {
            name: name.to_owned(),
            field_id,
            tx: self.state.tx.clone(),
            phantom: PhantomData,
        })
    }

    /// Create a new anonymous dataflow variable (field) within the active [`Actor`].
    pub fn field<T: Any + Send>(&mut self, initial_value: T) -> Arc<Field<T>> {
        self.named_field("", initial_value)
    }

    /// Retrieve a reference to the current value of a dataflow variable (field); if execution
    /// is currently within a [dataflow block][Activation::dataflow], marks the block as
    /// *depending upon* the field.
    ///
    pub fn get<T: Any + Send>(&mut self, field: &Field<T>) -> &T {
        if let Some(block) = self.active_block {
            tracing::trace!(?field, ?block, action = "get", "observed");
            self.state.dataflow.record_observation(block, field.field_id);
        }
        let any = self.state.fields.get(&field.field_id)
            .expect("Attempt to get() missing field: wrong actor?");
        any.downcast_ref().expect("Attempt to access field at incorrect type")
    }

    /// Retrieve a mutable reference to the contents of a dataflow variable (field). As for
    /// [`get`][Activation::get], if used within a dataflow block, marks the block as
    /// *depending upon* the field. In addition, because the caller may mutate the field, this
    /// function (pessimistically) marks the field as dirty, which will lead to later
    /// reevaluation of dependent blocks.
    ///
    pub fn get_mut<T: Any + Send>(&mut self, field: &Field<T>) -> &mut T {
        {
            // Overapproximation.
            if let Some(block) = self.active_block {
                tracing::trace!(?field, ?block, action = "get_mut", "observed");
                self.state.dataflow.record_observation(block, field.field_id);
            }
            tracing::trace!(?field, active_block = ?self.active_block, action = "get_mut", "damaged");
            self.state.dataflow.record_damage(field.field_id);
        }
        let any = self.state.fields.get_mut(&field.field_id)
            .expect("Attempt to get_mut() missing field: wrong actor?");
        any.downcast_mut().expect("Attempt to access field at incorrect type")
    }

    /// Overwrite the value of a dataflow variable (field). Marks the field as dirty, even if
    /// the new value is [`eq`][std::cmp::PartialEq::eq] to the value being overwritten.
    ///
    pub fn set<T: Any + Send>(&mut self, field: &Field<T>, value: T) {
        tracing::trace!(?field, active_block = ?self.active_block, action = "set", "damaged");
        // Overapproximation in many cases, since the new value may not produce an
        // observable difference (may be equal to the current value).
        self.state.dataflow.record_damage(field.field_id);
        let any = self.state.fields.get_mut(&field.field_id)
            .expect("Attempt to set() missing field: wrong actor?");
        *any = Box::new(value);
    }

    fn with_block(&mut self, block_id: BlockId, block: &mut Block) -> ActorResult {
        let saved = self.active_block.replace(block_id);
        let result = block(self);
        self.active_block = saved;
        result
    }

    /// Creates (and immediately executes) a new *dataflow block* that will be reexecuted if
    /// any of its *dependent fields* (accessed via e.g. [`get`][Activation::get] or
    /// [`get_mut`][Activation::get_mut]) are mutated.
    ///
    pub fn dataflow<F: 'static + Send + FnMut(&mut Activation) -> ActorResult>(&mut self, block: F) -> ActorResult {
        let block_id = BlockId::new(NEXT_BLOCK_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed))
            .expect("Internal error: Attempt to allocate BlockId of zero. Too many BlockIds allocated. Restart the process.");
        let mut block: Block = Box::new(block);
        self.with_block(block_id, &mut block)?;
        self.state.blocks.insert(block_id, (self.facet.facet_id, block));
        Ok(())
    }

    fn repair_dataflow(&mut self) -> Result<bool, Error> {
        let mut pass_number = 0;
        while !self.state.dataflow.is_clean() {
            pass_number += 1;
            tracing::trace!(?pass_number, "repair_dataflow");
            let damaged_field_ids = self.state.dataflow.take_damaged_nodes();
            for field_id in damaged_field_ids.into_iter() {
                let block_ids = self.state.dataflow.take_observers_of(&field_id);
                for block_id in block_ids.into_iter() {
                    if let Some((facet_id, mut block)) = self.state.blocks.remove(&block_id) {
                        let result = self.with_facet(
                            true, facet_id, |t| t.with_block(block_id, &mut block));
                        self.state.blocks.insert(block_id, (facet_id, block));
                        result?;
                    }
                }
            }
        }
        if pass_number > 0 {
            tracing::trace!(passes = ?pass_number, "repair_dataflow complete");
        }
        Ok(pass_number > 0)
    }

    fn _restore_invariants(&mut self) -> ActorResult {
        loop {
            loop {
                let actions = std::mem::take(&mut self.pending.for_myself);
                if actions.is_empty() { break; }
                for action in actions.into_iter() { action(self)? }
            }
            if !self.repair_dataflow()? {
                break;
            }
        }
        Ok(())
    }

    fn restore_invariants(&mut self, d: RunDisposition) -> RunDisposition {
        match d {
            RunDisposition::Continue =>
                self._restore_invariants().into(),
            RunDisposition::Terminate(Ok(())) =>
                RunDisposition::Terminate(self._restore_invariants()),
            RunDisposition::Terminate(Err(_)) =>
                d,
        }
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
        tracing::trace!("EventBuffer::deliver");
        if !self.for_myself.is_empty() {
            panic!("Unprocessed for_myself events remain at deliver() time");
        }
        for (_actor_id, (tx, turn)) in std::mem::take(&mut self.queues).into_iter() {
            // Deliberately ignore send errors here: they indicate that the recipient is no
            // longer alive. But we don't care about that case, since we have to be robust to
            // crashes anyway. (When we were printing errors we saw here, an example of the
            // problems it caused was a relay output_loop that received EPIPE causing the relay
            // to crash, just as it was receiving thousands of messages a second, leading to
            // many, many log reports of failed send_actions from the following line.)
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
        ACCOUNTS.write().insert(id, (name, Arc::clone(&debt)));
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
        ACCOUNTS.write().remove(&self.id);
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
    /// Create a new actor. It still needs to be [`boot`ed][Self::boot].
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        let actor_id = next_actor_id();
        let root = Facet::new(None);
        tracing::trace!(?actor_id, root_facet_id = ?root.facet_id, "Actor::new");
        let mut st = RunningActor {
            actor_id,
            tx,
            mailbox: Weak::new(),
            dataflow: Graph::new(),
            fields: HashMap::new(),
            blocks: HashMap::new(),
            exit_hooks: Vec::new(),
            cleanup_actions: Map::new(),
            facet_nodes: Map::new(),
            facet_children: Map::new(),
            root: root.facet_id,
        };
        st.facet_nodes.insert(root.facet_id, root);
        Actor {
            rx,
            ac_ref: ActorRef {
                actor_id,
                state: Arc::new(Mutex::new(ActorState::Running(st))),
            },
        }
    }

    fn link(self, t_parent: &mut Activation) -> Self {
        if t_parent.active_facet().is_none() {
            panic!("No active facet when calling spawn_link");
        }
        self.ac_ref.root_facet_ref().activate(Account::new(crate::name!("link")), |t_child| {
            t_parent.half_link(t_child);
            t_child.half_link(t_parent);
            Ok(())
        }).expect("Failed during link");
        self
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
            self.run(|t| {
                t.facet(boot)?;
                Ok(())
            }).await;
            let result = self.ac_ref.exit_status().expect("terminated");
            match &result {
                Ok(()) => tracing::trace!("normal stop"),
                Err(e) => tracing::error!("error stop: {}", e),
            }
            result
        }.instrument(name))
    }

    async fn run<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        boot: F,
    ) -> () {
        let root_facet_ref = self.ac_ref.root_facet_ref();

        let terminate = |result: ActorResult| {
            let _ = root_facet_ref.activate_exit(Account::new(crate::name!("shutdown")),
                                                 |_| RunDisposition::Terminate(result));
        };

        if root_facet_ref.activate(Account::new(crate::name!("boot")), boot).is_err() {
            return;
        }

        loop {
            tracing::trace!(actor_id = ?self.ac_ref.actor_id, "mainloop top");
            match self.rx.recv().await {
                None => {
                    return terminate(Err(error("Unexpected channel close", AnyValue::new(false))));
                }
                Some(m) => match m {
                    SystemMessage::Release => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id, "SystemMessage::Release");
                        return terminate(Ok(()));
                    }
                    SystemMessage::ReleaseField(field_id) => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id,
                                        "SystemMessage::ReleaseField({})", field_id);
                        self.ac_ref.access(|s| if let ActorState::Running(ra) = s {
                            ra.fields.remove(&field_id);
                        })
                    }
                    SystemMessage::Turn(mut loaned_item) => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id, "SystemMessage::Turn");
                        let actions = std::mem::take(&mut loaned_item.item);
                        let r = root_facet_ref.activate(Arc::clone(&loaned_item.account), |t| {
                            for action in actions.into_iter() { action(t)? }
                            Ok(())
                        });
                        if r.is_err() { return; }
                    }
                    SystemMessage::Crash(e) => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id,
                                        "SystemMessage::Crash({:?})", &e);
                        return terminate(Err(e));
                    }
                }
            }
        }
    }
}

impl Facet {
    fn new(parent_facet_id: Option<FacetId>) -> Self {
        Facet {
            facet_id: next_facet_id(),
            parent_facet_id,
            outbound_handles: Set::new(),
            stop_actions: Vec::new(),
            linked_tasks: Map::new(),
            inert_check_preventers: Arc::new(AtomicU64::new(0)),
        }
    }

    fn prevent_inert_check(&mut self) -> impl FnOnce() {
        let inert_check_preventers = Arc::clone(&self.inert_check_preventers);
        let armed = AtomicU64::new(1);
        inert_check_preventers.fetch_add(1, Ordering::Relaxed);
        move || {
            match armed.compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => {
                    inert_check_preventers.fetch_sub(1, Ordering::Relaxed);
                    ()
                }
                Err(_) => (),
            }
        }
    }

    fn retract_outbound(&mut self, t: &mut Activation) {
        for handle in std::mem::take(&mut self.outbound_handles).into_iter() {
            tracing::trace!(h = ?handle, "retract on termination");
            t.retract(handle);
        }
    }
}

impl ActorRef {
    /// Uses an internal mutex to access the internal state: takes the
    /// lock, calls `f` with the internal state, releases the lock,
    /// and returns the result of `f`.
    pub fn access<R, F: FnOnce(&mut ActorState) -> R>(&self, f: F) -> R {
        f(&mut *self.state.lock())
    }

    /// Retrieves the exit status of the denoted actor. If it is still
    /// running, yields `None`; otherwise, yields `Some(Ok(()))` if it
    /// exited normally, or `Some(Err(_))` if it terminated
    /// abnormally.
    pub fn exit_status(&self) -> Option<ActorResult> {
        self.access(|state| match state {
            ActorState::Running(_) => None,
            ActorState::Terminated { exit_status } => Some((**exit_status).clone()),
        })
    }

    fn facet_ref(&self, facet_id: FacetId) -> FacetRef {
        FacetRef {
            actor: self.clone(),
            facet_id,
        }
    }

    fn root_facet_id(&self) -> FacetId {
        self.access(|s| match s {
            ActorState::Terminated { .. } => panic!("Actor unexpectedly in terminated state"),
            ActorState::Running(ra) => ra.root, // what a lot of work to get this one number
        })
    }

    fn root_facet_ref(&self) -> FacetRef {
        self.facet_ref(self.root_facet_id())
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

    /// Registers the entity `r` in the list of exit hooks for this
    /// actor. When the actor terminates, `r`'s
    /// [`exit_hook`][Entity::exit_hook] will be called.
    pub fn add_exit_hook<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>) {
        let r = Arc::clone(r);
        self.exit_hooks.push(Box::new(
            move |t, exit_status| r.internal_with_entity(|e| e.exit_hook(t, &exit_status))))
    }

    fn get_facet(&mut self, facet_id: FacetId) -> Option<&mut Facet> {
        self.facet_nodes.get_mut(&facet_id)
    }

    /// See the definition of an [inert facet][Facet#inert-facets].
    fn facet_exists_and_is_inert(&mut self, facet_id: FacetId) -> bool {
        let no_kids = self.facet_children.get(&facet_id).map(|cs| cs.is_empty()).unwrap_or(true);
        if let Some(f) = self.get_facet(facet_id) {
            // The only outbound handle the root facet of an actor may have is a link
            // assertion, from [Activation::link]. This is not to be considered a "real"
            // assertion for purposes of keeping the facet alive!
            let no_outbound_handles = f.outbound_handles.is_empty();
            let is_root_facet = f.parent_facet_id.is_none();
            let no_linked_tasks = f.linked_tasks.is_empty();
            let no_inert_check_preventers = f.inert_check_preventers.load(Ordering::Relaxed) == 0;
            tracing::trace!(?facet_id, ?no_kids, ?no_outbound_handles, ?is_root_facet, ?no_linked_tasks, ?no_inert_check_preventers);
            no_kids && (no_outbound_handles || is_root_facet) && no_linked_tasks && no_inert_check_preventers
        } else {
            tracing::trace!(?facet_id, exists = ?false);
            false
        }
    }

    fn insert_retract_cleanup_action<M: 'static + Send>(
        &mut self,
        r: &Arc<Ref<M>>,
        handle: Handle,
    ) {
        let r = Arc::clone(r);
        self.cleanup_actions.insert(
            handle,
            CleanupAction::ForAnother(Arc::clone(&r.mailbox), Box::new(
                move |t| t.with_entity(&r, |t, e| {
                    tracing::trace!(?handle, "retracted");
                    if let Some(f) = t.active_facet() {
                        f.outbound_handles.remove(&handle);
                    }
                    e.retract(t, handle)
                }))));
    }
}

impl<T: Any + Send> std::fmt::Debug for Field<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "#<Field {:?} {}>", self.name, self.field_id)
    }
}

impl<T: Any + Send> Eq for Field<T> {}
impl<T: Any + Send> PartialEq for Field<T> {
    fn eq(&self, other: &Field<T>) -> bool {
        self.field_id == other.field_id
    }
}

impl<T: Any + Send> Drop for Field<T> {
    fn drop(&mut self) {
        let _ = self.tx.send(SystemMessage::ReleaseField(self.field_id));
        ()
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.rx.close();
        tracing::trace!(actor_id = ?self.ac_ref.actor_id, "Actor::drop");
    }
}

impl Drop for Facet {
    fn drop(&mut self) {
        for (_task_id, token) in std::mem::take(&mut self.linked_tasks).into_iter() {
            token.cancel();
        }

        if !self.outbound_handles.is_empty() {
            panic!("Internal error: outbound_handles not empty at drop time");
        }

        tracing::trace!(facet_id = ?self.facet_id, "Facet::drop");
    }
}

/// Directly injects `action` into `mailbox`, billing subsequent activity against `account`.
///
/// Primarily for use by [linked tasks][Activation::linked_task].
#[must_use]
pub fn external_event(mailbox: &Arc<Mailbox>, account: &Arc<Account>, action: Action) -> ActorResult {
    send_actions(&mailbox.tx, account, vec![action])
}

/// Directly injects `actions` into `mailbox`, billing subsequent activity against `account`.
///
/// Primarily for use by [linked tasks][Activation::linked_task].
#[must_use]
pub fn external_events(mailbox: &Arc<Mailbox>, account: &Arc<Account>, actions: PendingEventQueue) -> ActorResult {
    send_actions(&mailbox.tx, account, actions)
}

impl<M> Ref<M> {
    /// Supplies the behaviour (`e`) for a `Ref` created via
    /// [`create_inert`][Activation::create_inert].
    ///
    /// # Panics
    ///
    /// Panics if this `Ref` has already been given a behaviour.
    pub fn become_entity<E: 'static + Entity<M>>(&self, e: E) {
        let mut g = self.target.lock();
        if g.is_some() {
            panic!("Double initialization of Ref");
        }
        *g = Some(Box::new(e));
    }

    fn internal_with_entity<R, F: FnOnce(&mut dyn Entity<M>) -> R>(&self, f: F) -> R {
        let mut g = self.target.lock();
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
        write!(f, "⌜{}/{}:{:016x}⌝", self.mailbox.actor_id, self.facet_id, self.oid())
    }
}

impl Cap {
    /// Given a `Ref<M>`, where `M` is interconvertible with
    /// `AnyValue`, yields a `Cap` for the referenced entity. The
    /// `Cap` automatically decodes presented `AnyValue`s into
    /// instances of `M`.
    pub fn guard<L: 'static + Sync + Send, M: 'static + Send>(
        literals: Arc<L>,
        underlying: Arc<Ref<M>>,
    ) -> Arc<Self>
    where
        M: for<'a> Unparse<&'a L, AnyValue>,
        M: for<'a> Parse<&'a L, AnyValue>,
    {
        Self::new(&Arc::new(Ref {
            mailbox: Arc::clone(&underlying.mailbox),
            facet_id: underlying.facet_id,
            target: Mutex::new(Some(Box::new(Guard { underlying: underlying, literals }))),
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
    pub fn assert<L, M: Unparse<L, AnyValue>>(&self, t: &mut Activation, literals: L, m: &M) -> Option<Handle>
    {
        self.rewrite(m.unparse(literals)).map(|m| t.assert(&self.underlying, m))
    }

    /// `update` is to [`assert`] as [`Activation::update`] is to [`Activation::assert`].
    pub fn update<L, M: Unparse<L, AnyValue>>(
        &self,
        t: &mut Activation,
        handle: &mut Option<Handle>,
        literals: L,
        m: Option<&M>,
    ) {
        t.update(handle, &self.underlying, m.and_then(|m| self.rewrite(m.unparse(literals))))
    }

    /// Translates `m` into an `AnyValue`, passes it through
    /// [`rewrite`][Self::rewrite], and then sends it via method
    /// [`message`][Activation::message] on the activation `t`.
    pub fn message<L, M: Unparse<L, AnyValue>>(&self, t: &mut Activation, literals: L, m: &M)
    {
        if let Some(m) = self.rewrite(m.unparse(literals)) {
            t.message(&self.underlying, m)
        }
    }

    /// Synchronizes with the reference underlying the cap.
    pub fn sync(&self, t: &mut Activation, peer: Arc<Ref<Synced>>) {
        t.sync(&self.underlying, peer)
    }
}

impl std::fmt::Debug for Cap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if self.attenuation.is_empty() {
            self.underlying.fmt(f)
        } else {
            write!(f, "⌜{}/{}:{:016x}\\{:?}⌝",
                   self.underlying.mailbox.actor_id,
                   self.underlying.facet_id,
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

impl<L: Sync + Send, M> Entity<AnyValue> for Guard<L, M>
where
    M: for<'a> Unparse<&'a L, AnyValue>,
    M: for<'a> Parse<&'a L, AnyValue>,
{
    fn assert(&mut self, t: &mut Activation, a: AnyValue, h: Handle) -> ActorResult {
        match M::parse(&*self.literals, &a) {
            Ok(a) => t.with_entity(&self.underlying, |t, e| e.assert(t, a, h)),
            Err(_) => Ok(()),
        }
    }
    fn retract(&mut self, t: &mut Activation, h: Handle) -> ActorResult {
        t.with_entity(&self.underlying, |t, e| e.retract(t, h))
    }
    fn message(&mut self, t: &mut Activation, m: AnyValue) -> ActorResult {
        match M::parse(&*self.literals, &m) {
            Ok(m) => t.with_entity(&self.underlying, |t, e| e.message(t, m)),
            Err(_) => Ok(()),
        }
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        t.with_entity(&self.underlying, |t, e| e.sync(t, peer))
    }
    fn stop(&mut self, t: &mut Activation) -> ActorResult {
        t.with_entity(&self.underlying, |t, e| e.stop(t))
    }
    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        self.underlying.internal_with_entity(|e| e.exit_hook(t, exit_status))
    }
}

impl<M> Entity<M> for StopOnRetract {
    fn retract(&mut self, t: &mut Activation, _h: Handle) -> ActorResult {
        t.stop()
    }
}

impl<F: Send + FnMut(&mut Activation) -> ActorResult> Entity<Synced> for F {
    fn message(&mut self, t: &mut Activation, _m: Synced) -> ActorResult {
        self(t)
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

/// A convenient way of cloning a bunch of state shared among [entities][Entity], actions,
/// linked tasks, etc.
///
/// Directly drawn from the discussion [here](https://github.com/rust-lang/rfcs/issues/2407).
///
#[macro_export]
macro_rules! enclose {
    ( ( $($name:ident),* ) $closure:expr ) => {
        {
            $(let $name = $name.clone();)*
            $closure
        }
    }
}
