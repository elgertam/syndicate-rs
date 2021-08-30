#![doc = include_str!("../README.md")]

#[doc(inline)]
pub use preserves::value;

#[doc(inline)]
pub use preserves;

pub mod actor;
pub mod bag;
pub mod convert;
pub mod dataspace;
pub mod during;
pub mod error;
#[doc(hidden)]
pub mod pattern;
pub mod relay;
pub mod rewrite;
pub mod supervise;

pub mod schemas {
    //! Auto-generated codecs for [Syndicate protocol
    //! schemas](https://git.syndicate-lang.org/syndicate-lang/syndicate-protocols/src/branch/main/schemas).
    include!(concat!(env!("OUT_DIR"), "/src/schemas/mod.rs"));
}

pub mod skeleton;
pub mod sturdy;
pub mod tracer;

#[doc(inline)]
pub use during::entity;

#[doc(inline)]
pub use tracer::convenient_logging;
