#![doc = concat!(
    include_str!("../doc/actor.md"),
    include_str!("../doc/what-is-an-actor.md"),
    include_str!("../doc/flow-control.md"),
    include_str!("../doc/linked-tasks.md"),
)]

use crate::enclose;

use super::dataflow::Graph;
use super::error::Error;
use super::error::error;
use super::rewrite::CaveatError;
use super::rewrite::CheckedCaveat;
use super::schemas::protocol;
use super::schemas::sturdy;
use super::trace;

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

// use tracing::Instrument;

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

/// The type of the optional names attached to actors, tasks, and [`Account`]s.
pub type Name = Option<AnyValue>;

/// The type of process-unique actor IDs.
pub type ActorId = NonZeroU64;

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

/// Error responses to events must have type `ActorError`.
pub type ActorError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// Responses to events must have type `ActorResult`.
pub type ActorResult = Result<(), ActorError>;

/// Final exit status of an actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitStatus {
    Normal,
    Dropped,
    Error(Error),
}

impl From<ExitStatus> for Result<(), Error> {
    fn from(status: ExitStatus) -> Self {
        match status {
            ExitStatus::Normal => Ok(()),
            ExitStatus::Dropped => Ok(()),
            ExitStatus::Error(e) => Err(e),
        }
    }
}

impl From<ExitStatus> for ActorResult {
    fn from(status: ExitStatus) -> Self {
        Result::<(), Error>::from(status).map_err(|e| e.into())
    }
}

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
    /// cleanly. Will not be called in case of abnormal shutdown (crash) of an actor. Callbacks
    /// run in the context of the owning facet's *parent* facet.
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
    /// [Activation::add_exit_hook].
    ///
    /// The default implementation does nothing.
    fn exit_hook(&mut self, turn: &mut Activation, exit_status: &Arc<ExitStatus>) {
    }
}

/// An "inert" entity, that does nothing in response to any event delivered to it.
///
/// Useful as a placeholder or dummy in various situations.
pub struct InertEntity;
impl<M> Entity<M> for InertEntity {}

type TracedAction = (Option<trace::TargetedTurnEvent>, Action);
type Action = Box<dyn Send + FnOnce(&mut Activation) -> ActorResult>;
type Block = Box<dyn Send + FnMut(&mut Activation) -> ActorResult>;
type InfallibleAction = Box<dyn Send + FnOnce(&mut Activation)>;

#[doc(hidden)]
pub struct OutboundAssertion {
    established: bool,
    asserting_facet_id: FacetId,
    peer: Arc<Mailbox>,
    retractor: TracedAction,
}

#[doc(hidden)]
pub type OutboundAssertions = Map<Handle, Arc<Mutex<Option<OutboundAssertion>>>>;

#[doc(hidden)]
pub type PendingEventQueue = Vec<TracedAction>;

/// The main API for programming Syndicated Actor objects.
///
/// Through `Activation`s, programs can access the state of their
/// animating actor and their active [`Facet`].
///
/// Usually, an `Activation` will be supplied to code that needs one; but when non-Actor code
/// (such as a [linked task][crate::actor#linked-tasks]) needs to enter an Actor's execution
/// context, use [`FacetRef::activate`] to construct one.
///
/// Many actions that an entity can perform are methods directly on
/// `Activation`, but methods on [`FacetRef`] are also sometimes useful.
///
/// This is what other implementations call a "Turn", renamed here to
/// avoid conflicts with [`crate::schemas::protocol::Turn`].
pub struct Activation {
    // Do not poke at exposed but doc(hidden) fields here! You will violate invariants if you do!
    // They are exposed for debug/reflective purposes.

    //---------------------------------------------------------------------------
    // Fields related to an active turn for an actor.

    // INVARIANT: facet.is_none() any time the owning actor's Mutex is unlocked.
    facet_id: Option<FacetId>,
    account: Option<Arc<Account>>,
    turn_description: Option<trace::TurnDescription>,
    trace_collector: Option<trace::TraceCollector>,

    pre_commit_actions: Vec<Action>,
    rollback_actions: Vec<InfallibleAction>,
    commit_actions: Vec<InfallibleAction>,

    // INVARIANT: At most one of single_queue and multiple_queues is non-None at any given time.
    single_queue: Option<(ActorId, UnboundedSender<SystemMessage>, PendingEventQueue)>,
    multiple_queues: Option<HashMap<ActorId, (UnboundedSender<SystemMessage>, PendingEventQueue)>>,

    //---------------------------------------------------------------------------
    // Fields related to the actor itself, relevant even when it is suspended.

    actor_id: ActorId,
    actor_ref: ActorRef,

    tx: UnboundedSender<SystemMessage>,
    mailbox: Weak<Mailbox>,

    exit_hooks: Vec<Box<dyn Send + FnOnce(&mut Activation, &Arc<ExitStatus>)>>,
    #[doc(hidden)]
    pub outbound_assertions: OutboundAssertions,
    #[doc(hidden)]
    pub facet_nodes: Map<FacetId, Facet>,
    #[doc(hidden)]
    pub facet_children: Map<FacetId, Set<FacetId>>,
    pub root: FacetId, // TODO rename to root_facet_id, or ideally make it a FacetRef called root_facet

    #[doc(hidden)]
    dataflow: Option<Box<DataflowState>>
}

pub struct DataflowState {
    active_block: Option<BlockId>,
    graph: Graph<FieldId, BlockId>,
    #[doc(hidden)]
    pub fields: HashMap<FieldId, Box<dyn Any + Send>>,
    blocks: HashMap<BlockId, (FacetId, Block)>,
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
    trace_collector: Option<trace::TraceCollector>,
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
    Turn(Option<trace::TurnCause>, LoanedItem<PendingEventQueue>),
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
    trace_collector: Option<trace::TraceCollector>,
    ac_ref: ActorRef,
}

/// A reference to an actor's private [`ActorState`].
#[derive(Clone)]
pub struct ActorRef {
    /// The ID of the referenced actor.
    pub actor_id: ActorId,

    // Not intended for ordinary use! You'll break a bunch of invariants if you do!
    // Used internally, and made publicly accessible for reflective/debug use.
    #[doc(hidden)]
    pub state: Arc<Mutex<ActorState>>,
}

/// A combination of an [`ActorRef`] with a [`FacetId`], acting as a capability to enter the
/// execution context of a facet from a linked task.
#[derive(Debug, Clone)]
pub struct FacetRef {
    pub actor: ActorRef,
    pub facet_id: FacetId,
}

/// The state of an actor: either `Running` or `Terminated`.
pub enum ActorState {
    /// A non-terminated actor has an associated [`Activation`] state record.
    Running(Activation),
    /// A terminated actor has an [`ActorResult`] as its `exit_status`.
    Terminated {
        /// The exit status of the actor: `Ok(())` for normal
        /// termination, `Err(_)` for abnormal termination.
        exit_status: Arc<ExitStatus>,
    },
}

/// The type of process-unique task IDs.
pub type TaskId = u64;

/// Handle to a shared, mutable field (i.e. a *dataflow variable*) within a running actor.
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
/// A facet is considered *inert* if either:
///
/// 1. it has a parent facet and that parent facet is terminated; or
/// 2. it is either the root facet or its parent is not yet terminated, and it:
///    1. has no child facets;
///    2. has no cleanup actions (that is, no assertions placed by any of its entities);
///    3. has no linked tasks; and
///    4. has no "inert check preventers" (see [Activation::prevent_inert_check]).
///
/// If a facet is created and is inert at the moment that its `boot` function returns, it is
/// automatically terminated.
///
/// When a facet is terminated, if its parent facet is inert, the parent is terminated.
///
/// If the root facet in an actor is terminated, the entire actor is terminated (with exit
/// status `Ok(())`).
///
// Do not poke at exposed but doc(hidden) fields here! You will violate invariants if you do!
// They are exposed for debug/reflective purposes.
pub struct Facet {
    /// The ID of the facet.
    pub facet_id: FacetId,
    /// The ID of the facet's parent facet, if any; if None, this facet is the `Actor`'s root facet.
    pub parent_facet_id: Option<FacetId>,
    #[doc(hidden)]
    pub outbound_handles: Set<Handle>,
    stop_actions: Vec<Action>,
    #[doc(hidden)]
    pub linked_tasks: Map<TaskId, CancellationToken>,
    #[doc(hidden)]
    pub inert_check_preventers: Arc<AtomicU64>,
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

/// [Linked tasks][Activation::linked_task] terminate yielding values of this type.
pub enum LinkedTaskTermination {
    /// Causes the task's associated [Facet] to be [stop][Activation::stop]ped when the task
    /// returns.
    Normal,
    /// Causes no action to be taken regarding the task's associated [Facet] at the time the
    /// task returns.
    KeepFacet,
}

#[derive(Debug, Clone, Copy)]
enum TerminationDirection {
    BelowStartingPoint,
    AtOrAboveStartingPoint,
}

//---------------------------------------------------------------------------

const BUMP_AMOUNT: u8 = 10;

static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
#[doc(hidden)]
pub fn next_actor_id() -> ActorId {
    ActorId::new(NEXT_ACTOR_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed))
        .expect("Internal error: Attempt to allocate ActorId of zero. Too many ActorIds allocated. Restart the process.")
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

static NEXT_ACTIVATION_ID: AtomicU64 = AtomicU64::new(9);

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
    pub static ref ACCOUNTS: RwLock<Map<u64, (Name, Arc<AtomicI64>)>> = Default::default();

    #[doc(hidden)]
    pub static ref ACTORS: RwLock<Map<ActorId, (Name, ActorRef)>> = Default::default();
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

mod panic_guard {
    use super::*;
    pub struct PanicGuard(Option<UnboundedSender<SystemMessage>>);
    impl PanicGuard {
        pub(super) fn new(tx: UnboundedSender<SystemMessage>) -> Self {
            tracing::trace!("Panic guard armed");
            PanicGuard(Some(tx))
        }
        pub fn disarm(&mut self) {
            tracing::trace!("Panic guard disarmed");
            self.0 = None;
        }
    }
    impl Drop for PanicGuard {
        fn drop(&mut self) {
            if let Some(tx) = &self.0 {
                tracing::trace!("Panic guard triggering");
                let _ = tx.send(SystemMessage::Crash(
                    error("Actor panicked during activation", AnyValue::new(false))));
            }
        }
    }
}

impl FacetRef {
    /// Executes `f` in a new "[turn][Activation]" for `actor`. If `f` returns `Ok(())`,
    /// [commits the turn][Activation::commit] and performs the buffered actions; otherwise,
    /// abandons the turn and discards the buffered actions.
    ///
    /// Returns `true` if, at the end of the activation, `actor` had not yet terminated.
    ///
    /// Bills any activity to `account`.
    pub fn activate<F>(
        &self,
        account: &Arc<Account>,
        cause: Option<trace::TurnCause>,
        f: F,
    ) -> bool where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        let mut g = self.actor.state.lock();
        match &mut *g {
            ActorState::Terminated { .. } =>
                false,
            ActorState::Running(state) => {
                let mut panic_guard = panic_guard::PanicGuard::new(state.tx.clone());

                // let _entry = tracing::info_span!(parent: None, "actor", actor_id = ?self.actor.actor_id).entered();

                let maybe_exit_status = state.take_turn(
                    self.facet_id,
                    Arc::clone(account),
                    cause,
                    account.trace_collector.clone(),
                    |t| match f(t) {
                        Ok(()) =>
                            t.commit().map_or_else(
                                |e| Some(Err(e)),
                                |_| {
                                    // If we would otherwise continue, check the root facet: is it
                                    // still alive? If not, then the whole actor should terminate now.
                                    if let None = t.get_facet(t.root) {
                                        tracing::trace!(
                                            "terminating actor because root facet no longer exists");
                                        Some(Ok(()))
                                    } else {
                                        None
                                    }
                                }),
                        Err(e) => {
                            t.rollback();
                            Some(Err(e))
                        }
                    });

                panic_guard.disarm();
                drop(panic_guard);

                match maybe_exit_status {
                    None => true,
                    Some(result) => {
                        let exit_status = match result {
                            Ok(()) => ExitStatus::Normal,
                            Err(e) => ExitStatus::Error(e.into()),
                        };
                        g.terminate(exit_status, &self.actor, &account.trace_collector);
                        false
                    }
                }
            }
        }
    }
}

impl DataflowState {
    fn new() -> Self {
        DataflowState {
            active_block: None,
            graph: Graph::new(),
            fields: HashMap::new(),
            blocks: HashMap::new(),
        }
    }

    fn named_field<T: Any + Send>(
        &mut self,
        name: &str,
        initial_value: T,
        tx: UnboundedSender<SystemMessage>,
    ) -> Arc<Field<T>> {
        let field_id = FieldId::new(NEXT_FIELD_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed))
            .expect("Internal error: Attempt to allocate FieldId of zero. Too many FieldIds allocated. Restart the process.");
        self.fields.insert(field_id, Box::new(initial_value));
        Arc::new(Field {
            name: name.to_owned(),
            field_id,
            tx,
            phantom: PhantomData,
        })
    }

    fn get<T: Any + Send>(&mut self, field: &Field<T>) -> &T {
        if let Some(block) = self.active_block {
            tracing::trace!(?field, ?block, action = "get", "observed");
            self.graph.record_observation(block, field.field_id);
        }
        let any = self.fields.get(&field.field_id)
            .expect("Attempt to get() missing field: wrong actor?");
        any.downcast_ref().expect("Attempt to access field at incorrect type")
    }

    fn get_mut<T: Any + Send>(&mut self, field: &Field<T>) -> &mut T {
        {
            // Overapproximation.
            if let Some(block) = self.active_block {
                tracing::trace!(?field, ?block, action = "get_mut", "observed");
                self.graph.record_observation(block, field.field_id);
            }
            tracing::trace!(?field, active_block = ?self.active_block, action = "get_mut", "damaged");
            self.graph.record_damage(field.field_id);
        }
        let any = self.fields.get_mut(&field.field_id)
            .expect("Attempt to get_mut() missing field: wrong actor?");
        any.downcast_mut().expect("Attempt to access field at incorrect type")
    }

    fn set<T: Any + Send>(&mut self, field: &Field<T>, value: T) {
        tracing::trace!(?field, active_block = ?self.active_block, action = "set", "damaged");
        // Overapproximation in many cases, since the new value may not produce an
        // observable difference (may be equal to the current value).
        self.graph.record_damage(field.field_id);
        let any = self.fields.get_mut(&field.field_id)
            .expect("Attempt to set() missing field: wrong actor?");
        *any = Box::new(value);
    }
}

impl Activation {
    fn take_turn<R, F: FnOnce(&mut Self) -> R>(
        &mut self,
        facet_id: FacetId,
        account: Arc<Account>,
        cause: Option<trace::TurnCause>,
        trace_collector: Option<trace::TraceCollector>,
        f: F,
    ) -> R {
        if self.facet_id.is_some() {
            panic!("Invariant violated: nested turns detected");
        }
        self.facet_id = Some(facet_id);
        self.account = Some(account);
        self.turn_description = cause.map(|c| trace::TurnDescription::new(
            NEXT_ACTIVATION_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed),
            c));
        self.trace_collector = trace_collector;
        let result = f(self);
        self.facet_id = None;
        self.account = None;
        self.turn_description = None;
        self.trace_collector = None;
        result
    }

    pub fn trace_collector(&self) -> Option<trace::TraceCollector> {
        self.trace_collector.clone()
    }

    /// Constructs a new [`Account`] with the given `name`, inheriting
    /// its `trace_collector` from the current [`Activation`]'s cause.
    pub fn create_account(&self, name: Name) -> Arc<Account> {
        Account::new(name, self.trace_collector())
    }

    fn with_facet<F>(&mut self, facet_id: FacetId, f: F) -> ActorResult
    where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        if self.facet_nodes.contains_key(&facet_id) {
            tracing::trace!(facet_id, "alive=true");
            self._with_facet(facet_id, f)
        } else {
            tracing::trace!(facet_id, "alive=false");
            Ok(())
        }
    }

    /// Retrieve the [`FacetId`] of the currently-active facet.
    pub fn facet_id(&self) -> FacetId {
        self.facet_id.expect("Attempt to access active facet ID outside turn")
    }

    /// Retrieve a [`FacetRef`] for the currently-active facet.
    pub fn facet_ref(&self) -> FacetRef {
        FacetRef {
            actor: self.actor_ref.clone(),
            facet_id: self.facet_id(),
        }
    }

    fn _with_facet<F>(&mut self, facet_id: FacetId, f: F) -> ActorResult
    where
        F: FnOnce(&mut Activation) -> ActorResult,
    {
        let saved = self.facet_id.replace(facet_id);
        // let _entry = tracing::info_span!("facet", ?facet_id).entered();
        let result = f(self);
        self.facet_id = saved;
        result
    }

    #[doc(hidden)]
    pub fn with_entity<M, F>(&mut self, r: &Arc<Ref<M>>, f: F) -> ActorResult where
        F: FnOnce(&mut Activation, &mut dyn Entity<M>) -> ActorResult
    {
        self.with_facet(r.facet_id, |t| r.internal_with_entity(|e| f(t, e)))
    }

    fn active_facet<'a>(&'a mut self) -> Option<&'a mut Facet> {
        self.get_facet(self.facet_id())
    }

    /// Retrieves the chain of facet IDs, in order, from the currently-active [`Facet`] up to
    /// and including the root facet of the active actor. Useful for debugging.
    pub fn facet_ids(&mut self) -> Vec<FacetId> {
        if let Some(f) = self.get_facet_immut(self.facet_id()) {
            self.facet_ids_for(f)
        } else {
            Vec::new()
        }
    }

    fn facet_ids_for<'a>(&self, f: &'a Facet) -> Vec<FacetId> {
        let mut ids = Vec::new();
        ids.push(f.facet_id);
        let mut id = f.parent_facet_id;
        while let Some(parent_id) = id {
            ids.push(parent_id);
            match self.get_facet_immut(parent_id) {
                None => break,
                Some(pf) => id = pf.parent_facet_id,
            }
        }
        ids
    }

    #[inline(always)]
    fn trace<F: FnOnce(&mut Self) -> trace::ActionDescription>(&mut self, f: F) {
        if self.turn_description.is_some() {
            let a = f(self);
            self.turn_description.as_mut().unwrap().record(a);
        }
    }

    #[inline(always)]
    fn trace_targeted<M, F: FnOnce() -> trace::TurnEvent>(
        &mut self,
        internal: bool,
        r: &Arc<Ref<M>>,
        f: F,
    ) -> Option<trace::TargetedTurnEvent> {
        self.turn_description.as_mut().map(|d| {
            let event = trace::TargetedTurnEvent {
                target: r.as_ref().into(),
                detail: f(),
            };
            d.record(if internal {
                trace::ActionDescription::EnqueueInternal { event: Box::new(event.clone()) }
            } else {
                trace::ActionDescription::Enqueue { event: Box::new(event.clone()) }
            });
            event
        })
    }

    fn insert_outbound_assertion<M: 'static + Send>(
        &mut self,
        r: &Arc<Ref<M>>,
        handle: Handle,
        description: Option<trace::TargetedTurnEvent>,
    ) -> bool {
        let asserting_facet_id = self.facet_id();
        match self.get_facet(asserting_facet_id) {
            None => false,
            Some(f) => {
                f.outbound_handles.insert(handle);

                let r = Arc::clone(r);
                let details = OutboundAssertion {
                    established: false,
                    asserting_facet_id,
                    peer: Arc::clone(&r.mailbox),
                    retractor: (
                        description,
                        Box::new(move |remote_t| remote_t.with_entity(&r, |t, e| {
                            tracing::trace!(?handle, "retracted");
                            e.retract(t, handle)
                        }))),
                };

                self.outbound_assertions.insert(handle, Arc::new(Mutex::new(Some(details))));

                self.on_rollback(move |t| {
                    if let Some(f) = t.get_facet(asserting_facet_id) {
                        f.outbound_handles.remove(&handle);
                    }
                    t.outbound_assertions.remove(&handle);
                });
                self.on_commit(move |t| {
                    if let Some(oa_handle) = t.outbound_assertions.get_mut(&handle) {
                        oa_handle.lock().as_mut().expect("OutboundAssertion").established = true;
                    }
                });

                true
            }
        }
    }

    /// Core API: assert `a` at recipient `r`.
    ///
    /// Returns the [`Handle`] for the new assertion.
    pub fn assert<M: 'static + Send + std::fmt::Debug>(&mut self, r: &Arc<Ref<M>>, a: M) -> Handle {
        let handle = next_handle();
        if self.insert_outbound_assertion(r, handle, self.turn_description.as_ref().map(
            enclose!((r) move |_| trace::TargetedTurnEvent {
                target: r.as_ref().into(),
                detail: trace::TurnEvent::Retract {
                    handle: Box::new(protocol::Handle(handle.into())),
                },
            })))
        {
            tracing::trace!(?r, ?handle, ?a, "assert");
            let r = Arc::clone(r);
            let description = self.trace_targeted(false, &r, || trace::TurnEvent::Assert {
                assertion: Box::new((&a).into()),
                handle: Box::new(protocol::Handle(handle.into())),
            });
            self.queue_for(&r).push((
                description,
                Box::new(move |t| t.with_entity(&r, |t, e| {
                    tracing::trace!(?handle, ?a, "asserted");
                    e.assert(t, a, handle)
                }))));
        }
        handle
    }

    fn half_link(&mut self, t_other: &mut Activation) -> Handle {
        let this_actor_id = self.actor_id;
        let entity_ref = t_other.create::<AnyValue, _>(StopOnRetract);
        let handle = next_handle();
        assert!(self.insert_outbound_assertion(&entity_ref, handle, self.turn_description.as_ref().map(
            enclose!((entity_ref) move |_| trace::TargetedTurnEvent {
                target: entity_ref.as_ref().into(),
                detail: trace::TurnEvent::BreakLink {
                    source: Box::new(this_actor_id.into()),
                    handle: Box::new(protocol::Handle(handle.into())),
                },
            }))));
        tracing::trace!(?handle, ?entity_ref, "half_link");
        t_other.with_entity(&entity_ref, |t, e| e.assert(t, AnyValue::new(true), handle)).unwrap();
        handle
    }

    /// Core API: retract a previously-established assertion.
    pub fn retract(&mut self, handle: Handle) {
        tracing::trace!(?handle, "retract");
        if let Some(oa_handle) = self.outbound_assertions.remove(&handle) {
            self.on_rollback(enclose!((oa_handle) move |t| {
                if oa_handle.lock().as_mut().expect("OutboundAssertion").established {
                    t.outbound_assertions.insert(handle, oa_handle);
                }
            }));

            let g = oa_handle.lock();
            let oa = g.as_ref().expect("Present OutboundAssertion");

            if let Some(desc) = &oa.retractor.0 {
                self.trace(|_| trace::ActionDescription::Enqueue { event: Box::new(desc.clone()) });
            }

            self.queue_for_mailbox(&oa.peer).push((
                oa.retractor.0.clone(),
                Box::new(enclose!((oa_handle) move |remote_t| {
                    let oa = oa_handle.lock().take().expect("Present OutboundAssertion");
                    (oa.retractor.1)(remote_t)
                }))));

            let asserting_facet_id = oa.asserting_facet_id;
            self.on_commit(move |t| {
                if let Some(f) = t.get_facet(asserting_facet_id) {
                    f.outbound_handles.remove(&handle);
                }
            });
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
        let description = self.trace_targeted(false, &r, || trace::TurnEvent::Message {
            body: Box::new((&m).into()),
        });
        self.queue_for(&r).push((
            description,
            Box::new(move |t| t.with_entity(&r, |t, e| {
                tracing::trace!(?m, "delivered");
                e.message(t, m)
            }))))
    }

    /// Core API: begins a synchronisation with `r`.
    ///
    /// Once the synchronisation request reaches `r`'s actor, it will
    /// send a response to `peer`, which acts as a continuation for
    /// the synchronisation request.
    pub fn sync<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>, peer: Arc<Ref<Synced>>) {
        let r = Arc::clone(r);
        let description = self.trace_targeted(false, &r, || trace::TurnEvent::Sync {
            peer: Box::new(peer.as_ref().into()),
        });
        self.queue_for(&r).push((
            description,
            Box::new(move |t| t.with_entity(&r, |t, e| e.sync(t, peer)))))
    }

    pub fn later<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(&mut self, action: F) {
        // TODO: properly describe this in traces
        let mailbox = self.mailbox();
        self.queue_for_mailbox(&mailbox).push((
            None,
            Box::new(action)));
    }

    /// Registers the entity `r` in the list of stop actions for the active facet. If the facet
    /// terminates cleanly, `r`'s [`stop`][Entity::stop] will be called in the context of the
    /// facet's parent.
    ///
    /// **Note.** If the actor crashes, stop actions will *not* be called.
    ///
    /// Use [`Activation::add_exit_hook`] to install a callback that will be called at the
    /// end of the lifetime of the *actor* rather than the facet. (Also, exit hooks are called
    /// no matter whether actor termination was normal or abnormal.)
    pub fn on_stop_notify<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>) {
        if let Some(f) = self.active_facet() {
            let r = Arc::clone(r);
            f.stop_actions.push(Box::new(move |t| r.internal_with_entity(|e| e.stop(t))));
        }
    }

    /// Registers `action` in the list of stop actions for the active facet. If the facet
    /// terminates cleanly, `action` will be called in the context of the facet's parent. See
    /// also notes against [`on_stop_notify`][Activation::on_stop_notify].
    pub fn on_stop<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(&mut self, action: F) {
        self.on_facet_stop(self.facet_id(), action)
    }

    fn on_facet_stop<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        facet_id: FacetId,
        action: F,
    ) {
        if let Some(f) = self.get_facet(facet_id) {
            f.stop_actions.push(Box::new(action));
        }
    }

    /// Retrieve the [`Account`] against which actions are recorded.
    pub fn account(&self) -> &Arc<Account> {
        self.account.as_ref().expect("Attempt to access active account outside turn")
    }

    /// Delivers all pending actions in this activation and resets it, ready for more. Succeeds
    /// iff all [pre-commit][Activation::pre_commit] actions succeed.
    ///
    /// # Commit procedure
    ///
    /// If an [activation][FacetRef::activate]'s `f` function returns successfully, the
    /// activation *commits* according to the following procedure:
    ///
    ///  1. While the dataflow graph needs repairing or outstanding
    ///     [pre-commit][Activation::pre_commit`] actions exist:
    ///       1. repair the dataflow graph
    ///       2. run all pre-commit actions
    ///     Note that graph repair or a pre-commit action may fail, causing the commit to
    ///     abort, or may further damage the dataflow graph or schedule another pre-commit
    ///     action, causing another go around the loop.
    ///
    ///  2. The commit becomes final. All queued events are sent; all internal accounting
    ///     actions are performed.
    pub fn commit(&mut self) -> ActorResult {
        tracing::trace!("Activation::commit");
        loop {
            let mut should_loop = false;
            if self.repair_dataflow()? {
                should_loop = true;
            }
            if !self.pre_commit_actions.is_empty() {
                should_loop = true;
                for ac in std::mem::take(&mut self.pre_commit_actions) {
                    ac(self)?
                }
            }
            if !should_loop {
                break;
            }
        }
        tracing::trace!("Commit is final");
        if !self.rollback_actions.is_empty() {
            // just drop 'em so they don't run next time
            std::mem::take(&mut self.rollback_actions);
        }
        if !self.commit_actions.is_empty() {
            for ac in std::mem::take(&mut self.commit_actions) {
                ac(self);
            }
        }

        let mut causing_turn_id: Option<trace::TurnId> = None;
        if let Some(d) = self.turn_description.take() {
            causing_turn_id = Some(d.id.clone());
            if let Some(c) = &self.trace_collector {
                c.record(self.actor_id, trace::ActorActivation::Turn(Box::new(d)));
            }
        }

        // Deliberately ignore send errors here: they indicate that the recipient is no
        // longer alive. But we don't care about that case, since we have to be robust to
        // crashes anyway. (When we were printing errors we saw here, an example of the
        // problems it caused was a relay output_loop that received EPIPE causing the relay
        // to crash, just as it was receiving thousands of messages a second, leading to
        // many, many log reports of failed send_actions from the following line.)
        if let Some((_actor_id, tx, turn)) = std::mem::take(&mut self.single_queue) {
            let desc = causing_turn_id.as_ref().map(|id| trace::TurnCause::Turn { id: Box::new(id.clone()) });
            let _ = send_actions(&tx, desc, &self.account(), turn);
        }
        if let Some(table) = std::mem::take(&mut self.multiple_queues) {
            for (_actor_id, (tx, turn)) in table.into_iter() {
                let desc = causing_turn_id.as_ref().map(|id| trace::TurnCause::Turn { id: Box::new(id.clone()) });
                let _ = send_actions(&tx, desc, &self.account(), turn);
            }
        }

        tracing::trace!("Activation::commit complete");
        Ok(())
    }

    fn rollback(&mut self) {
        tracing::trace!("Activation::rollback");
        if !self.pre_commit_actions.is_empty() {
            // just drop 'em so they don't run next time
            std::mem::take(&mut self.pre_commit_actions);
        }
        if !self.rollback_actions.is_empty() {
            for ac in std::mem::take(&mut self.rollback_actions) {
                ac(self)
            }
        }
        if !self.commit_actions.is_empty() {
            // just drop 'em so they don't run next time
            std::mem::take(&mut self.commit_actions);
        }
        tracing::trace!("Activation::rollback complete");
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
            mailbox: self.mailbox(),
            facet_id: self.facet_id(),
            target: Mutex::new(None),
        })
    }

    /// Start a new [linked task][crate::actor#linked-tasks] attached to the active facet. The
    /// task will execute the future "`boot`" to completion unless it is cancelled first (by
    /// e.g. termination of the owning facet or crashing of the owning actor). Stops the active
    /// facet when the linked task completes. Uses `name` for log messages emitted by the task.
    pub fn linked_task<F: 'static + Send + futures::Future<Output = Result<LinkedTaskTermination, Error>>>(
        &mut self,
        name: Name,
        boot: F,
    ) {
        let mailbox = self.mailbox();
        let facet = self.facet_ref();
        let trace_collector = self.trace_collector();
        if self.active_facet().is_some() {
            let task_id = NEXT_TASK_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed);
            self.trace(|_| trace::ActionDescription::LinkedTaskStart {
                task_name: Box::new(name.into()),
                id: Box::new(trace::TaskId(AnyValue::new(task_id))),
            });

            let f = self.active_facet().unwrap();
            let token = CancellationToken::new();
            {
                let token = token.clone();
                tokio::spawn(async move {
                    tracing::trace!(task_id, "linked task start");
                    let (result, reason) = select! {
                        _ = token.cancelled() => {
                            tracing::trace!(task_id, "linked task cancelled");
                            (LinkedTaskTermination::Normal, trace::LinkedTaskReleaseReason::Cancelled)
                        }
                        result = boot => match result {
                            Ok(t) => {
                                tracing::trace!(task_id, "linked task normal stop");
                                (t, trace::LinkedTaskReleaseReason::Normal)
                            }
                            Err(e) => {
                                tracing::error!(task_id, "linked task error: {}", e);
                                let _ = mailbox.tx.send(SystemMessage::Crash(e.clone()));
                                Err(e)?
                            }
                        }
                    };
                    let release_account =
                        Account::new(Some(AnyValue::symbol("linked-task-release")), trace_collector);
                    facet.activate(
                        &release_account,
                        release_account.trace_collector.as_ref().map(
                            |_| trace::TurnCause::LinkedTaskRelease {
                                id: Box::new(trace::TaskId(AnyValue::new(task_id))),
                                reason: Box::new(reason),
                            }),
                        |t| {
                            if let Some(f) = t.active_facet() {
                                tracing::trace!(task_id, "cancellation token removed");
                                f.linked_tasks.remove(&task_id);
                            }
                            if let LinkedTaskTermination::Normal = result {
                                t.stop();
                            }
                            Ok(())
                        });
                    Ok::<(), Error>(())
                });
                // }.instrument(tracing::info_span!("task", ?task_id).or_current()));
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
        let account = Arc::clone(self.account());
        let desc = self.turn_description.as_ref().map(|d| trace::TurnCause::Delay {
            causing_turn: Box::new(d.id.clone()),
            amount: duration.as_secs_f64().into(),
        });
        let instant = time::Instant::now() + duration;
        let facet = self.facet_ref();
        self.linked_task(Some(AnyValue::symbol("delay")), async move {
            tokio::time::sleep_until(instant.into()).await;
            facet.activate(&account, desc, a);
            Ok(LinkedTaskTermination::KeepFacet)
        });
    }

    /// Executes the given action immediately, and then every time another multiple of the
    /// given duration has elapsed (so long as the active facet still exists at that time).
    pub fn every<F: 'static + Send + FnMut(&mut Activation) -> ActorResult>(
        &mut self,
        duration: time::Duration,
        mut a: F,
    ) -> ActorResult {
        let account = Arc::clone(self.account());
        let facet = self.facet_ref();
        let desc = trace::TurnCause::PeriodicActivation { period: duration.as_secs_f64().into() };
        self.linked_task(Some(AnyValue::symbol("periodic-activation")), async move {
            let mut timer = tokio::time::interval(duration);
            loop {
                timer.tick().await;
                if !facet.activate(&account, Some(desc.clone()), |t| a(t)) {
                    break;
                }
            }
            Ok(LinkedTaskTermination::Normal)
        });
        Ok(())
    }

    /// Executes the given action at the given instant (so long as the active facet still
    /// exists at that time).
    pub fn at<I: Into<tokio::time::Instant>, F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        instant: I,
        a: F,
    ) {
        let delay = instant.into().checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(time::Duration::ZERO);
        self.after(delay, a)
    }

    /// Schedules the given action to run just prior to [commit][Activation::commit].
    pub fn pre_commit<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        action: F,
    ) {
        self.pre_commit_actions.push(Box::new(action));
    }

    fn on_rollback<F: 'static + Send + FnOnce(&mut Activation)>(&mut self, action: F) {
        self.rollback_actions.push(Box::new(action));
    }

    fn on_commit<F: 'static + Send + FnOnce(&mut Activation)>(&mut self, action: F) {
        self.commit_actions.push(Box::new(action));
    }

    /// Schedule the creation of a new actor wrapping an entity.
    pub fn spawn_for_entity<M>(
        &mut self,
        name: Name,
        link: bool,
        target: Box<dyn Entity<M>>,
    ) -> (Option<Arc<Ref<M>>>, ActorRef) {
        let ac_ref = self._spawn(name, |t| {
            let _ = t.prevent_inert_check();
            Ok(())
        }, link);
        let mut g = ac_ref.state.lock();
        let r = match &mut *g {
            ActorState::Terminated { .. } => None,
            ActorState::Running(state) => Some(Arc::new(Ref {
                mailbox: state.mailbox(),
                facet_id: state.root,
                target: Mutex::new(Some(target)),
            })),
        };
        drop(g);
        (r, ac_ref)
    }

    /// Schedule the creation of a new actor when the Activation commits.
    pub fn spawn<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        name: Name,
        boot: F,
    ) -> ActorRef {
        self._spawn(name, boot, false)
    }

    /// Schedule the creation of a new actor when the Activation commits.
    ///
    /// The new actor will be "linked" to the active facet: if the new actor terminates, the
    /// active facet is stopped, and if the active facet stops, the new actor's root facet is
    /// stopped.
    pub fn spawn_link<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        name: Name,
        boot: F,
    ) -> ActorRef {
        self._spawn(name, boot, true)
    }

    fn _spawn<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        name: Name,
        boot: F,
        link: bool,
    ) -> ActorRef {
        let ac = Actor::new(Some(self.actor_id), self.trace_collector());
        let ac_ref = ac.ac_ref.clone();
        self.trace(|_| trace::ActionDescription::Spawn {
            link,
            id: Box::new(ac_ref.actor_id.into()),
        });
        let cause = self.turn_description.as_ref().map(
            |d| trace::TurnCause::Turn { id: Box::new(d.id.clone()) });
        let ac = if link { ac.link(self) } else { ac };
        self.on_commit(move |t| {
            ac.boot(name, Arc::clone(t.account()), cause, boot);
        });
        ac_ref
    }

    /// Create a new subfacet of the currently-active facet. Runs `boot` in the new facet's
    /// context. If `boot` returns leaving the new facet [inert][Facet#inert-facets], the new
    /// facet is [stopped][Activation::stop_facet].
    pub fn facet<F: FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        boot: F,
    ) -> Result<FacetId, ActorError> {
        let f = Facet::new(Some(self.facet_id()));
        self.trace(|t| trace::ActionDescription::FacetStart {
            path: t.facet_ids_for(&f).iter().map(|i| (*i).into()).collect(),
        });
        let facet_id = f.facet_id;
        self.facet_nodes.insert(facet_id, f);
        tracing::trace!(parent_id = ?self.facet_id,
                        ?facet_id,
                        new_actor_facet_count = ?self.facet_nodes.len());
        self.facet_children.entry(self.facet_id()).or_default().insert(facet_id);
        self._with_facet(facet_id, move |t| {
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

    /// If `continuation` is supplied, adds it as a stop action for the [`Facet`] named by
    /// `facet_id`. Then, cleanly stops the facet immediately, without waiting for `self` to
    /// commit.
    pub fn stop_facet_and_continue<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        facet_id: FacetId,
        continuation: Option<F>,
    ) -> ActorResult {
        if let Some(k) = continuation {
            self.on_facet_stop(facet_id, k);
        }
        self._terminate_facet(facet_id,
                              TerminationDirection::AtOrAboveStartingPoint,
                              trace::FacetStopReason::ExplicitAction)
    }

    /// Cleanly stops the [`Facet`] named by `facet_id`.
    ///
    /// Equivalent to `self.stop_facet_and_continue(facet_id, None)`, except that the lack of a
    /// continuation means that there's no need for this method to return `ActorResult`.
    pub fn stop_facet(&mut self, facet_id: FacetId) {
        self.stop_facet_and_continue::<Action>(facet_id, None)
            .expect("Non-failing stop_facet_and_continue")
    }

    /// Cleanly stops the active facet.
    ///
    /// Equivalent to `self.stop_facet(self.facet_id())`.
    pub fn stop(&mut self) {
        self.stop_facet(self.facet_id())
    }

    /// Cleanly stops the active actor's root facet.
    /// This is one way to arrange a clean shutdown of the entire actor.
    ///
    /// Equivalent to `self.stop_facet(self.state.root)`.
    pub fn stop_root(&mut self) {
        self.stop_facet(self.root);
    }

    fn stop_if_inert(&mut self) {
        let facet_id = self.facet_id();
        // Registering a pre-commit hook lets this run after the dataflow graph has been repaired.
        self.pre_commit(move |t| {
            tracing::trace!("Checking inertness of facet {} from facet {}", facet_id, t.facet_id());
            if t.facet_exists_and_is_inert(facet_id) {
                tracing::trace!(" - facet {} is inert, stopping it", facet_id);
                t._terminate_facet(facet_id,
                                   TerminationDirection::AtOrAboveStartingPoint,
                                   trace::FacetStopReason::Inert)?;
            } else {
                tracing::trace!(" - facet {} is not inert", facet_id);
            }
            Ok(())
        })
    }

    fn _terminate_facet(
        &mut self,
        facet_id: FacetId,
        direction: TerminationDirection,
        reason: trace::FacetStopReason,
    ) -> ActorResult {
        if let Some(mut f) = self.facet_nodes.remove(&facet_id) {
            let maybe_parent_id = f.parent_facet_id;

            self.trace(|t| trace::ActionDescription::FacetStop {
                path: t.facet_ids_for(&f).iter().map(|i| (*i).into()).collect(),
                reason: Box::new(reason),
            });
            tracing::trace!(remaining_actor_facet_count = ?self.facet_nodes.len(),
                            ?facet_id,
                            ?direction,
                            "stopping");

            if let Some(children) = self.facet_children.remove(&facet_id) {
                for child_id in children.into_iter() {
                    self._terminate_facet(child_id,
                                          TerminationDirection::BelowStartingPoint,
                                          trace::FacetStopReason::ParentStopping)?;
                }
            }

            if let TerminationDirection::AtOrAboveStartingPoint = direction {
                if let Some(p) = maybe_parent_id {
                    self.facet_children.get_mut(&p).map(|children| children.remove(&facet_id));
                }
            }

            self._with_facet(maybe_parent_id.unwrap_or(facet_id), |t| {
                for ac in std::mem::take(&mut f.stop_actions).into_iter() {
                    ac(t)?
                }
                Ok(())
            })?;

            for handle in std::mem::take(&mut f.outbound_handles).into_iter() {
                tracing::trace!(h = ?handle, "retract on termination");
                self.retract(handle);
            }

            if let TerminationDirection::AtOrAboveStartingPoint = direction {
                match maybe_parent_id {
                    Some(p) => {
                        if self.facet_exists_and_is_inert(p) {
                            tracing::trace!("terminating parent {:?} of facet {:?}", p, facet_id);
                            self._terminate_facet(p,
                                                  TerminationDirection::AtOrAboveStartingPoint,
                                                  trace::FacetStopReason::Inert)?;
                        } else {
                            tracing::trace!("not terminating parent {:?} of facet {:?}", p, facet_id);
                        }
                    }
                    None => tracing::trace!("no parent of root facet {:?} to terminate", facet_id),
                }
            }
        }
        Ok(())
    }

    fn dataflow_state_mut(&mut self) -> &mut DataflowState {
        if self.dataflow.is_none() {
            self.dataflow = Some(Box::new(DataflowState::new()));
        }
        self.dataflow.as_mut().unwrap()
    }

    /// Create a new named dataflow variable (field) within the active [`Actor`].
    pub fn named_field<T: Any + Send>(&mut self, name: &str, initial_value: T) -> Arc<Field<T>> {
        let tx = self.tx.clone();
        self.dataflow_state_mut().named_field(name, initial_value, tx)
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
        self.dataflow_state_mut().get(field)
    }

    /// Retrieve a mutable reference to the contents of a dataflow variable (field). As for
    /// [`get`][Activation::get], if used within a dataflow block, marks the block as
    /// *depending upon* the field. In addition, because the caller may mutate the field, this
    /// function (pessimistically) marks the field as dirty, which will lead to later
    /// reevaluation of dependent blocks.
    ///
    pub fn get_mut<T: Any + Send>(&mut self, field: &Field<T>) -> &mut T {
        self.dataflow_state_mut().get_mut(field)
    }

    /// Overwrite the value of a dataflow variable (field). Marks the field as dirty, even if
    /// the new value is [`eq`][std::cmp::PartialEq::eq] to the value being overwritten.
    ///
    pub fn set<T: Any + Send>(&mut self, field: &Field<T>, value: T) {
        self.dataflow_state_mut().set(field, value)
    }

    fn with_block(&mut self, block_id: BlockId, block: &mut Block) -> ActorResult {
        let saved = self.dataflow_state_mut().active_block.replace(block_id);
        let result = block(self);
        self.dataflow_state_mut().active_block = saved;
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
        let facet_id = self.facet_id();
        self.dataflow_state_mut().blocks.insert(block_id, (facet_id, block));
        Ok(())
    }

    fn repair_dataflow(&mut self) -> Result<bool, ActorError> {
        if self.dataflow.is_none() {
            return Ok(false);
        }

        let mut pass_number = 0;
        loop {
            let mut blocks_to_run = HashMap::new();

            {
                let df = self.dataflow.as_mut().unwrap();
                if df.graph.is_clean() {
                    break;
                }
                pass_number += 1;
                tracing::trace!(?pass_number, "repair_dataflow");

                let damaged_field_ids = df.graph.take_damaged_nodes();
                for field_id in damaged_field_ids.into_iter() {
                    let block_ids = df.graph.take_observers_of(&field_id);
                    for block_id in block_ids.into_iter() {
                        if let Some(entry) = df.blocks.remove(&block_id) {
                            blocks_to_run.insert(block_id, entry);
                        }
                    }
                }
            }

            let mut error = None;
            let mut blocks_to_replace = Vec::with_capacity(blocks_to_run.len());
            for (block_id, (facet_id, mut block)) in blocks_to_run.into_iter() {
                if error.is_none() {
                    if let Err(e) = self.with_facet(facet_id, |t| t.with_block(block_id, &mut block)) {
                        error = Some(e);
                    }
                }
                blocks_to_replace.push((block_id, (facet_id, block)));
            }

            {
                let df = self.dataflow.as_mut().unwrap();
                for (block_id, entry) in blocks_to_replace.into_iter() {
                    df.blocks.insert(block_id, entry);
                }
            }

            if let Some(e) = error {
                Err(e)?;
            }
        }
        if pass_number > 0 {
            tracing::trace!(passes = ?pass_number, "repair_dataflow complete");
        }
        Ok(pass_number > 0)
    }

    fn queue_for<M>(&mut self, r: &Arc<Ref<M>>) -> &mut PendingEventQueue {
        self.queue_for_mailbox(&r.mailbox)
    }

    fn queue_for_mailbox(&mut self, mailbox: &Arc<Mailbox>) -> &mut PendingEventQueue {
        if self.multiple_queues.is_some() {
            return &mut self.multiple_queues.as_mut().unwrap().entry(mailbox.actor_id)
                .or_insert((mailbox.tx.clone(), Vec::with_capacity(3))).1;
        }

        if let None = self.single_queue {
            self.single_queue = Some((mailbox.actor_id, mailbox.tx.clone(), Vec::with_capacity(3)));
            return &mut self.single_queue.as_mut().unwrap().2;
        }

        if Some(mailbox.actor_id) == self.single_queue.as_ref().map(|e| e.0) {
            return &mut self.single_queue.as_mut().unwrap().2;
        }

        let (aid, tx, q) = std::mem::take(&mut self.single_queue).unwrap();
        let mut table = HashMap::new();
        table.insert(aid, (tx, q));
        self.multiple_queues = Some(table);
        &mut self.multiple_queues.as_mut().unwrap().entry(mailbox.actor_id)
            .or_insert((mailbox.tx.clone(), Vec::with_capacity(3))).1
    }

    /// Retrieve the ID of the current actor.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    #[doc(hidden)]
    pub fn actor_ref(&self) -> ActorRef {
        self.actor_ref.clone()
    }

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

    /// Registers the entity `r` in the list of exit hooks for this actor. When the actor
    /// terminates, `r`'s [`exit_hook`][Entity::exit_hook] will be called in the context of the
    /// actor's root facet.
    pub fn add_exit_hook<M: 'static + Send>(&mut self, r: &Arc<Ref<M>>) {
        let r = Arc::clone(r);
        self.exit_hooks.push(Box::new(
            move |t, exit_status| r.internal_with_entity(|e| e.exit_hook(t, &exit_status))))
    }

    fn get_facet(&mut self, facet_id: FacetId) -> Option<&mut Facet> {
        self.facet_nodes.get_mut(&facet_id)
    }

    fn get_facet_immut(&self, facet_id: FacetId) -> Option<&Facet> {
        self.facet_nodes.get(&facet_id)
    }

    /// See the definition of an [inert facet][Facet#inert-facets].
    fn facet_exists_and_is_inert(&mut self, facet_id: FacetId) -> bool {
        let no_kids = self.facet_children.get(&facet_id).map(|cs| cs.is_empty()).unwrap_or(true);
        if let Some(f) = self.get_facet(facet_id) {
            // The only outbound handle the root facet of an actor may have is a link
            // assertion, from [Activation::link]. This is not to be considered a "real"
            // assertion for purposes of keeping the facet alive!
            let maybe_parent_id = f.parent_facet_id.clone();
            let no_outbound_handles = f.outbound_handles.is_empty();
            let is_root_facet = maybe_parent_id.is_none();
            let no_linked_tasks = f.linked_tasks.is_empty();
            let no_inert_check_preventers = f.inert_check_preventers.load(Ordering::Relaxed) == 0;
            let parent_facet_missing = maybe_parent_id.map_or(false, |p| self.get_facet_immut(p).is_none());
            tracing::trace!(?facet_id, ?no_kids, ?no_outbound_handles, ?is_root_facet, ?no_linked_tasks, ?no_inert_check_preventers, ?parent_facet_missing);
            parent_facet_missing || (no_kids && (no_outbound_handles || is_root_facet) && no_linked_tasks && no_inert_check_preventers)
        } else {
            tracing::trace!(?facet_id, exists = ?false);
            false
        }
    }

    fn cleanup(
        mut self,
        exit_status: Arc<ExitStatus>,
        trace_collector: Option<trace::TraceCollector>,
    ) {
        match &*exit_status {
            ExitStatus::Normal => assert!(self.get_facet(self.root).is_none()),
            ExitStatus::Dropped | ExitStatus::Error(_) => (),
        }

        let cause = Some(trace::TurnCause::Cleanup);
        let account = Account::new(Some(AnyValue::symbol("cleanup")), trace_collector.clone());

        self.take_turn(
            self.root,
            account,
            cause,
            trace_collector,
            |t| {
                // NB. In descending order so that we retract newer handles first. Since
                // facet-tree-order retraction is children before parents, this isn't quite right - a
                // parent facet could have made an assertion after some child made an assertion. But
                // given that this is for *unclean shutdown*, maybe it's OK?
                //
                let handles_descending: Vec<Handle> = t.outbound_assertions.keys().rev().cloned().collect();
                tracing::trace!(actor_id=?t.actor_id,
                                handles=?handles_descending,
                                "remaining to retract at cleanup time");
                for handle in handles_descending.into_iter() {
                    t.retract(handle);
                }

                if let Err(err) = t.commit() {
                    // This can only happen through an internal error in this module
                    tracing::error!(?err, "Internal error during Activation::cleanup");
                }

                for action in std::mem::take(&mut t.exit_hooks) {
                    action(t, &exit_status);
                }
            });
    }
}

impl Account {
    /// Construct a new `Account`, storing `name` within it for
    /// debugging use.
    pub fn new(name: Name, trace_collector: Option<trace::TraceCollector>) -> Arc<Self> {
        let id = NEXT_ACCOUNT_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed);
        let debt = Arc::new(AtomicI64::new(0));
        ACCOUNTS.write().insert(id, (name, Arc::clone(&debt)));
        Arc::new(Account {
            id,
            debt,
            notify: Notify::new(),
            trace_collector,
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
    caused_by: Option<trace::TurnCause>,
    account: &Arc<Account>,
    t: PendingEventQueue,
) -> ActorResult {
    let token_count = t.len();
    Ok(tx.send(SystemMessage::Turn(caused_by, LoanedItem::new(account, token_count, t)))
       .map_err(|_| error("Target actor not running", AnyValue::new(false)))?)
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
        self.actor_id.cmp(&other.actor_id)
    }
}

impl PartialOrd for Mailbox {
    fn partial_cmp(&self, other: &Mailbox) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        tracing::debug!("Last reference to mailbox of actor id {:?} was dropped", self.actor_id);
        let _ = self.tx.send(SystemMessage::Release);
    }
}

impl Actor {
    /// Create and start a new "top-level" actor: an actor not
    /// causally related to another. This is the usual way to start a
    /// Syndicate program.
    pub fn top<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        trace_collector: Option<trace::TraceCollector>,
        boot: F,
    ) -> ActorHandle {
        let ac = Actor::new(None, trace_collector.clone());
        let topcause = trace_collector.as_ref().map(|_| trace::TurnCause::external("top-level actor"));
        let account = Account::new(None, trace_collector);
        ac.boot(None, account, topcause, boot)
    }

    /// Create a new actor. It still needs to be [`boot`ed][Self::boot].
    pub fn new(parent_actor_id: Option<ActorId>, trace_collector: Option<trace::TraceCollector>) -> Self {
        let (tx, rx) = unbounded_channel();
        let actor_id = next_actor_id();
        let root = Facet::new(None);
        tracing::debug!(?actor_id, ?parent_actor_id, root_facet_id = ?root.facet_id, "Actor::new");
        let mut st = Activation {
            facet_id: None,
            account: None,
            turn_description: None,
            trace_collector: None,

            pre_commit_actions: Vec::new(),
            rollback_actions: Vec::new(),
            commit_actions: Vec::new(),

            single_queue: None,
            multiple_queues: None,

            actor_id,
            actor_ref: ActorRef {
                actor_id,
                state: Arc::new(Mutex::new(ActorState::Terminated{
                    exit_status: Arc::new(ExitStatus::Normal)
                })),
            },

            tx,
            mailbox: Weak::new(),

            exit_hooks: Vec::new(),
            outbound_assertions: Map::new(),
            facet_nodes: Map::new(),
            facet_children: Map::new(),
            root: root.facet_id,

            dataflow: None,
        };
        st.facet_nodes.insert(root.facet_id, root);
        let ac_ref = st.actor_ref.clone();
        *ac_ref.state.lock() = ActorState::Running(st);
        Actor { rx, trace_collector, ac_ref }
    }

    fn link(self, t_parent: &mut Activation) -> Self {
        if t_parent.active_facet().is_none() {
            panic!("No active facet when calling spawn_link");
        }
        let account = Arc::clone(t_parent.account());
        let mut h_to_child = None;
        let mut h_to_parent = None;
        let is_alive = self.ac_ref.root_facet_ref().activate(
            &account,
            None,
            |t_child| {
                h_to_child = Some(t_parent.half_link(t_child));
                h_to_parent = Some(t_child.half_link(t_parent));
                Ok(())
            });
        if is_alive {
            let parent_actor = t_parent.actor_id;
            t_parent.trace(|_| trace::ActionDescription::Link {
                parent_actor: Box::new(parent_actor.into()),
                parent_to_child: Box::new(protocol::Handle(h_to_child.unwrap().into())),
                child_actor: Box::new(self.ac_ref.actor_id.into()),
                child_to_parent: Box::new(protocol::Handle(h_to_parent.unwrap().into())),
            });
            self
        } else {
            panic!("spawn_link'd actor terminated before link could happen");
        }
    }

    /// Start the actor's mainloop. Takes ownership of `self`. The
    /// `name` is used as context for any log messages emitted by the
    /// actor. The `boot` function is called in the actor's context,
    /// and then the mainloop is entered.
    pub fn boot<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        mut self,
        name: Name,
        boot_account: Arc<Account>,
        boot_cause: Option<trace::TurnCause>,
        boot: F,
    ) -> ActorHandle {
        let actor_id = self.ac_ref.actor_id;
        ACTORS.write().insert(actor_id, (name.clone(), self.ac_ref.clone()));
        let trace_collector = boot_account.trace_collector.clone();
        if let Some(c) = &trace_collector {
            c.record(actor_id, trace::ActorActivation::Start {
                actor_name: Box::new(name.into()),
            });
        }
        tokio::spawn(async move {
            tracing::trace!(?actor_id, "start");
            self.run(boot_account, boot_cause, move |t| {
                t.facet(boot)?;
                Ok(())
            }).await;
            tracing::trace!(?actor_id, "stop");
            self.ac_ref.exit_status().expect("terminated")
        })
        // }.instrument(tracing::info_span!(parent: None, "actor", ?actor_id).or_current()))
    }

    async fn run<F: 'static + Send + FnOnce(&mut Activation) -> ActorResult>(
        &mut self,
        boot_account: Arc<Account>,
        boot_cause: Option<trace::TurnCause>,
        boot: F,
    ) -> () {
        let root_facet_ref = self.ac_ref.root_facet_ref();

        let terminate = |e: Error | {
            assert!(!root_facet_ref.activate(&Account::new(None, None), None, |_| Err(e)?));
        };

        if !root_facet_ref.activate(&boot_account, boot_cause, boot) {
            return;
        }

        'mainloop: loop {
            tracing::trace!(actor_id = ?self.ac_ref.actor_id, "mainloop top");
            match self.rx.recv().await {
                None => {
                    terminate(error("Unexpected channel close", AnyValue::new(false)));
                    break 'mainloop;
                }
                Some(m) => match m {
                    SystemMessage::Release => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id, "SystemMessage::Release");
                        assert!(!root_facet_ref.activate(&Account::new(None, None), None, |t| {
                            t.stop_root();
                            Ok(())
                        }));
                        break 'mainloop;
                    }
                    SystemMessage::ReleaseField(field_id) => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id,
                                        "SystemMessage::ReleaseField({})", field_id);
                        self.ac_ref.access(|s| if let ActorState::Running(ra) = s {
                            ra.dataflow_state_mut().fields.remove(&field_id);
                        })
                    }
                    SystemMessage::Turn(cause, mut loaned_item) => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id, "SystemMessage::Turn");
                        let actions = std::mem::take(&mut loaned_item.item);
                        if !root_facet_ref.activate(
                            &loaned_item.account, cause, |t| {
                                for (maybe_desc, action) in actions.into_iter() {
                                    if let Some(desc) = maybe_desc {
                                        t.trace(|_| trace::ActionDescription::Dequeue {
                                            event: Box::new(desc),
                                        });
                                    }
                                    action(t)?;
                                }
                                Ok(())
                            })
                        {
                            break 'mainloop;
                        }
                    }
                    SystemMessage::Crash(e) => {
                        tracing::trace!(actor_id = ?self.ac_ref.actor_id,
                                        "SystemMessage::Crash({:?})", &e);
                        terminate(e);
                        break 'mainloop;
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
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // otherwise we get 'warning: field `0` is never read'
pub struct KeepAlive(Option<Arc<Mailbox>>);

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
            ActorState::Terminated { exit_status } => Some((**exit_status).clone().into()),
        })
    }

    /// Creates a [`KeepAlive`] for (usually temporarily) ensuring an [Actor] does not get
    /// garbage-collected due to no references to its [Mailbox] being held. (It may of course
    /// be terminated for other reasons.)
    pub fn keep_alive(&self) -> KeepAlive {
        KeepAlive(self.access(|s| match s {
            ActorState::Terminated { .. } => None,
            ActorState::Running(ra) => Some(ra.mailbox()),
        }))
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

impl std::fmt::Debug for ActorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "#<ActorRef {}>", self.actor_id)
    }
}

impl ActorState {
    fn is_running(&self) -> bool {
        match self {
            ActorState::Terminated { .. } => false,
            ActorState::Running(_) => true,
        }
    }

    fn terminate(
        &mut self,
        exit_status: ExitStatus,
        actor: &ActorRef,
        trace_collector: &Option<trace::TraceCollector>,
    ) {
        if !self.is_running() {
            return;
        }

        let exit_status = Arc::new(exit_status);

        let final_state = ActorState::Terminated { exit_status: Arc::clone(&exit_status) };
        match std::mem::replace(self, final_state) {
            ActorState::Terminated { .. } =>
                unreachable!(),
            ActorState::Running(state) =>
                state.cleanup(Arc::clone(&exit_status), trace_collector.clone()),
        }

        match &*exit_status {
            ExitStatus::Normal => tracing::trace!(actor_id=?actor.actor_id, "normal stop"),
            ExitStatus::Dropped => tracing::debug!(actor_id=?actor.actor_id, "force-terminated by Actor::drop"),
            ExitStatus::Error(e) => tracing::error!(actor_id=?actor.actor_id, %e, "error stop"),
        }

        if let Some(c) = trace_collector {
            c.record(actor.actor_id, trace::ActorActivation::Stop {
                status: Box::new(match &*exit_status {
                    ExitStatus::Normal | ExitStatus::Dropped => trace::ExitStatus::Ok,
                    ExitStatus::Error(e) => trace::ExitStatus::Error(Box::new(e.clone())),
                }),
            });
        }
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
        ACTORS.write().remove(&self.ac_ref.actor_id);
        // let _scope = tracing::info_span!(parent: None, "actor", actor_id = ?self.ac_ref.actor_id).entered();
        let mut g = self.ac_ref.state.lock();
        if g.is_running() {
            g.terminate(ExitStatus::Dropped, &self.ac_ref, &self.trace_collector);
        }
        tracing::debug!("Actor::drop");
    }
}

impl Drop for Facet {
    fn drop(&mut self) {
        for (_task_id, token) in std::mem::take(&mut self.linked_tasks).into_iter() {
            token.cancel();
        }

        if !self.outbound_handles.is_empty() {
            tracing::warn!(
                concat!("outbound_handles for {:?} not empty at drop time; ",
                        "retractions will happen at actor termination, ",
                        "but may not follow facet-tree order"),
                self.facet_id);
        }

        tracing::trace!(facet_id = ?self.facet_id, "Facet::drop");
    }
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
        // let _entry = tracing::info_span!("entity", r = ?self).entered();
        f(g.as_mut().expect("initialized").as_mut())
    }
}

impl<M> Ref<M> {
    /// Retrieves a process-unique identifier for the `Ref`; `Ref`s
    /// are compared by this identifier.
    pub fn oid(&self) -> usize {
        std::ptr::addr_of!(*self) as usize
    }

    pub fn debug_str(&self) -> String {
        format!("{}/{}:{:016x}", self.mailbox.actor_id, self.facet_id, self.oid())
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
        write!(f, "⌜{}⌝", self.debug_str())
    }
}

impl Cap {
    /// Given a `Ref<M>`, where `M` is interconvertible with
    /// `AnyValue`, yields a `Cap` for the referenced entity. The
    /// `Cap` automatically decodes presented `AnyValue`s into
    /// instances of `M`.
    pub fn guard<L: 'static + Sync + Send, M: 'static + Send>(
        literals: &Arc<L>,
        underlying: Arc<Ref<M>>,
    ) -> Arc<Self>
    where
        M: for<'a> Unparse<&'a L, AnyValue>,
        M: for<'a> Parse<&'a L, AnyValue>,
    {
        let literals = Arc::clone(literals);
        Self::new(&Arc::new(Ref {
            mailbox: Arc::clone(&underlying.mailbox),
            facet_id: underlying.facet_id,
            target: Mutex::new(Some(Box::new(Guard { underlying, literals }))),
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
    /// existing attenuation of `self` to the new `Cap` and adding the
    /// `caveats` to it.
    pub fn attenuate(&self, caveats: &[sturdy::Caveat]) -> Result<Arc<Self>, CaveatError> {
        let mut r = Cap { attenuation: self.attenuation.clone(), .. self.clone() };
        r.attenuation.extend(sturdy::Caveat::check_many(caveats)?);
        Ok(Arc::new(r))
    }

    /// Applies the contained attenuation to `a`, yielding `None` if
    /// `a` is filtered out, or `Some(_)` if it is accepted (and
    /// possibly transformed).
    pub fn rewrite(&self, mut a: AnyValue) -> Option<AnyValue> {
        for c in self.attenuation.iter().rev() {
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

    pub fn debug_str(&self) -> String {
        if self.attenuation.is_empty() {
            self.underlying.debug_str()
        } else {
            format!("{}/{}:{:016x}\\{:?}",
                    self.underlying.mailbox.actor_id,
                    self.underlying.facet_id,
                    self.underlying.oid(),
                    self.attenuation)
        }
    }
}

impl std::fmt::Debug for Cap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "⌜{}⌝", self.debug_str())
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
    fn exit_hook(&mut self, t: &mut Activation, exit_status: &Arc<ExitStatus>) {
        self.underlying.internal_with_entity(|e| e.exit_hook(t, exit_status))
    }
}

impl<M> Entity<M> for StopOnRetract {
    fn retract(&mut self, t: &mut Activation, _h: Handle) -> ActorResult {
        Ok(t.stop())
    }
}

impl<F: Send + FnMut(&mut Activation) -> ActorResult> Entity<Synced> for F {
    fn message(&mut self, t: &mut Activation, _m: Synced) -> ActorResult {
        self(t)
    }
}

async fn wait_loop(wait_time: time::Duration) {
    let deadline = time::Instant::now() + wait_time;
    while time::Instant::now() < deadline {
        let remaining_count = ACTORS.read().len();
        if remaining_count == 0 {
            break;
        }
        tracing::debug!("Waiting for {} remaining actors to stop", remaining_count);
        tokio::time::sleep(time::Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_all_actors_to_stop(wait_time: time::Duration) {
    wait_loop(wait_time).await;
    let remaining = ACTORS.read().clone();
    if remaining.len() > 0 {
        tracing::warn!("Some actors remain after {:?}:", wait_time);
        for (name, actor) in remaining.into_values() {
            tracing::warn!(?name, ?actor.actor_id, "actor still running, requesting shutdown");
            let g = actor.state.lock();
            if let ActorState::Running(state) = &*g {
                state.shutdown();
            }
        }
        wait_loop(wait_time).await;
        let remaining = ACTORS.read().clone();
        if remaining.len() > 0 {
            tracing::error!("Some actors failed to stop after being explicitly shut down:");
            for (name, actor) in remaining.into_values() {
                tracing::error!(?name, ?actor.actor_id, "actor failed to stop");
            }
        } else {
            tracing::debug!("All remaining actors have stopped.");
        }
    } else {
        tracing::debug!("All remaining actors have stopped.");
    }
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
