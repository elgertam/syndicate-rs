#![doc = include_str!("../README.md")]
#![feature(min_specialization)]

#[doc(inline)]
pub use preserves::value;

#[doc(inline)]
pub use preserves;

#[doc(inline)]
pub use preserves_schema;

pub mod actor;
pub mod bag;
pub mod dataflow;
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
pub mod trace;

#[doc(inline)]
pub use during::entity;

/// Sets up [`tracing`] logging in a reasonable way.
///
/// Useful at the top of `main` functions.
pub fn convenient_logging() -> actor::ActorResult {
    let filter = match std::env::var(tracing_subscriber::filter::EnvFilter::DEFAULT_ENV) {
        Err(std::env::VarError::NotPresent) =>
            tracing_subscriber::filter::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
        _ =>
            tracing_subscriber::filter::EnvFilter::try_from_default_env()?,
    };
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing global subscriber");
    Ok(())
}

/// Retrieve the version of the Syndicate crate.
pub fn syndicate_package_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

preserves_schema::define_language!(language(): Language<actor::AnyValue> {
    syndicate: schemas::Language,
});

#[cfg(test)]
mod protocol_test {
    use crate::*;
    use preserves::value::{BytesBinarySource, BinarySource, IOValueDomainCodec, ViaCodec, IOValue};
    use preserves_schema::Deserialize;

    #[test] fn decode_sync() {
        let input_str = "[[2 <sync #![0 11]>]]";
        let mut src = BytesBinarySource::new(input_str.as_bytes());
        let mut r = src.text::<IOValue, _>(ViaCodec::new(IOValueDomainCodec));
        let packet: schemas::protocol::Packet<IOValue> = schemas::protocol::Packet::deserialize(&mut r).unwrap();
        println!("{:?}", packet);
    }
}
