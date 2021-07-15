use crate::actor::*;

use preserves::value::NestedValue;

use std::sync::Arc;

struct Tracer(tracing::Span);

fn set_name_oid(_ac: &mut Actor, t: &mut Tracer, r: &Arc<Ref>) {
    t.0.record("oid", &tracing::field::display(&r.addr.oid.0));
}

pub fn tracer(ac: &mut Actor, name: tracing::Span) -> Arc<Ref> {
    ac.create_rec(Tracer(name), set_name_oid)
}

pub fn tracer_top(name: tracing::Span) -> Arc<Ref> {
    Actor::create_and_start_rec(crate::name!(parent: None, "tracer"), Tracer(name), set_name_oid)
}

impl Entity for Tracer {
    fn assert(&mut self, _t: &mut Activation, a: _Any, h: Handle) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(a = debug(&a), h = debug(&h), "assert");
        Ok(())
    }
    fn retract(&mut self, _t: &mut Activation, h: Handle) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(h = debug(&h), "retract");
        Ok(())
    }
    fn message(&mut self, _t: &mut Activation, m: _Any) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(m = debug(&m), "message");
        Ok(())
    }
    fn sync(&mut self, t: &mut Activation, peer: Arc<Ref>) -> ActorResult {
        let _guard = self.0.enter();
        tracing::trace!(peer = debug(&peer), "sync");
        t.message(&peer, _Any::new(true));
        Ok(())
    }
}

pub fn convenient_logging() -> Result<(), Box<dyn std::error::Error>> {
    let filter = match std::env::var(tracing_subscriber::filter::EnvFilter::DEFAULT_ENV) {
        Err(std::env::VarError::NotPresent) =>
            tracing_subscriber::filter::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
        _ =>
            tracing_subscriber::filter::EnvFilter::try_from_default_env()?,
    };
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(true)
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing global subscriber");
    Ok(())
}
