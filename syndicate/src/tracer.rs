use crate::actor::*;

use std::fmt::Debug;
use std::io;
use std::sync::Arc;

struct Tracer(tracing::Span);

fn set_name_oid<M>(t: &mut Tracer, r: &Arc<Ref<M>>) {
    t.0.record("oid", &tracing::field::display(&r.oid()));
}

pub fn tracer<M: Debug>(t: &mut Activation, name: tracing::Span) -> Arc<Ref<M>> {
    let mut e = Tracer(name);
    let r = t.create_inert();
    set_name_oid(&mut e, &r);
    r.become_entity(e);
    r
}

impl<M: Debug> Entity<M> for Tracer {
    fn assert(&mut self, _t: &mut Activation, a: M, h: Handle) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(?a, ?h, "assert");
        Ok(())
    }
    fn retract(&mut self, _t: &mut Activation, h: Handle) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(?h, "retract");
        Ok(())
    }
    fn message(&mut self, _t: &mut Activation, m: M) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(?m, "message");
        Ok(())
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref<Synced>>) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(?peer, "sync");
        t.message(&peer, Synced);
        Ok(())
    }
}

/// Sets up [`tracing`] logging in a reasonable way.
///
/// Useful at the top of `main` functions.
pub fn convenient_logging() -> Result<(), Box<dyn std::error::Error>> {
    let filter = match std::env::var(tracing_subscriber::filter::EnvFilter::DEFAULT_ENV) {
        Err(std::env::VarError::NotPresent) =>
            tracing_subscriber::filter::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
        _ =>
            tracing_subscriber::filter::EnvFilter::try_from_default_env()?,
    };
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(true)
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing global subscriber");
    Ok(())
}
