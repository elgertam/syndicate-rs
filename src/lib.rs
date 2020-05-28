#![recursion_limit="512"]

pub mod bag;
pub mod config;
pub mod dataspace;
pub mod packets;
pub mod peer;
pub mod skeleton;
pub mod spaces;

pub use preserves::value;

// use std::sync::atomic::{AtomicUsize, Ordering};
//
// #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
// pub enum Syndicate {
//     Placeholder(usize),
// }
//
// impl value::Domain for Syndicate {}
//
// static NEXT_PLACEHOLDER: AtomicUsize = AtomicUsize::new(0);
// impl Syndicate {
//     pub fn new_placeholder() -> Self {
//         Self::Placeholder(NEXT_PLACEHOLDER.fetch_add(1, Ordering::SeqCst))
//     }
// }

pub type ConnId = u64;
pub type V = value::IOValue; // value::ArcValue<Syndicate>;
