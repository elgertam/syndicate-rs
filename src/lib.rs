#![recursion_limit="512"]

pub use preserves::value;

pub use schemas::internal_protocol::Handle;
pub use schemas::internal_protocol::Oid;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub mod actor;
pub mod bag;
pub mod config;
pub mod dataspace;
pub mod error;
pub mod pattern;
pub mod relay;
pub mod schemas;
pub mod skeleton;

pub type Assertion = schemas::dataspace::_Any;

pub type ActorId = u64;
static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(0);
pub fn next_actor_id() -> ActorId {
    NEXT_ACTOR_ID.fetch_add(1, Ordering::Relaxed)
}

static NEXT_OID: AtomicU64 = AtomicU64::new(0);
pub fn next_oid() -> Oid {
    Oid(value::signed_integer::SignedInteger::from(
        NEXT_OID.fetch_add(1, Ordering::Relaxed) as u128))
}

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(0);
pub fn next_handle() -> Handle {
    Handle(value::signed_integer::SignedInteger::from(
        NEXT_HANDLE.fetch_add(1, Ordering::Relaxed) as u128))
}

static NEXT_MAILBOX_ID: AtomicU64 = AtomicU64::new(0);
pub fn next_mailbox_id() -> u64 {
    NEXT_MAILBOX_ID.fetch_add(1, Ordering::Relaxed)
}
