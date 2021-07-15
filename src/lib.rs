pub use preserves::value;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use actor::Handle;
use actor::Oid;

pub mod actor;
pub mod bag;
pub mod config;
pub mod dataspace;
pub mod during;
pub mod error;
pub mod pattern;
pub mod relay;
pub mod rewrite;
pub mod schemas;
pub mod skeleton;
pub mod sturdy;
pub mod tracer;

pub use during::entity;

pub use tracer::tracer;
pub use tracer::tracer_top;
pub use tracer::convenient_logging;

pub type ActorId = u64;

const BUMP_AMOUNT: u8 = 10;

static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);
pub fn next_actor_id() -> ActorId {
    NEXT_ACTOR_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed)
}

static NEXT_OID: AtomicU64 = AtomicU64::new(2);
pub fn next_oid() -> Oid {
    Oid(value::signed_integer::SignedInteger::from(
        NEXT_OID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed) as u128))
}

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(3);
pub fn next_handle() -> Handle {
    Handle(value::signed_integer::SignedInteger::from(
        NEXT_HANDLE.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed) as u128))
}

static NEXT_MAILBOX_ID: AtomicU64 = AtomicU64::new(4);
pub fn next_mailbox_id() -> u64 {
    NEXT_MAILBOX_ID.fetch_add(BUMP_AMOUNT.into(), Ordering::Relaxed)
}
