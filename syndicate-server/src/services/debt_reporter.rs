use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services::DebtReporter;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("debt_reporter"), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: DebtReporter>, |t: &mut Activation| {
            t.spawn_link(tracing::Span::current(), enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: DebtReporter) -> ActorResult {
    ds.assert(t, language(), &lifecycle::started(&spec));
    ds.assert(t, language(), &lifecycle::ready(&spec));
    t.every(core::time::Duration::from_millis((spec.interval_seconds.0 * 1000.0) as u64), |_t| {
        for (id, (name, debt)) in syndicate::actor::ACCOUNTS.read().iter() {
            let _enter = name.enter();
            tracing::info!(id, debt = ?debt.load(std::sync::atomic::Ordering::Relaxed));
        }
        Ok(())
    })
}
