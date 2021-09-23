use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::preserves_schema::Codec;

use crate::language::language;
use crate::schemas::internal_services::DebtReporter;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, ds, language(), <require-service $_spec: DebtReporter>, |t: &mut Activation| {
            t.spawn_link(tracing::Span::current(), enclose!((ds) |t| run(t, ds)));
            Ok(())
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>) -> ActorResult {
    let spec = language().unparse(&DebtReporter);
    ds.assert(t, &(), &syndicate_macros::template!("<service-running =spec>"));
    t.linked_task(syndicate::name!("tick"), async {
        let mut timer = tokio::time::interval(core::time::Duration::from_secs(1));
        loop {
            timer.tick().await;
            for (id, (name, debt)) in syndicate::actor::ACCOUNTS.read().unwrap().iter() {
                let _enter = name.enter();
                tracing::info!(id, debt = ?debt.load(std::sync::atomic::Ordering::Relaxed));
            }
        }
    });
    Ok(())
}
