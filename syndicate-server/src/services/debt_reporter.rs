use std::sync::Arc;

use syndicate::actor::*;
use syndicate::convert::*;
use syndicate::during::entity;
use syndicate::schemas::dataspace::Observe;

use crate::schemas::internal_services;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        let monitor = entity(())
            .on_asserted_facet({
                let ds = Arc::clone(&ds);
                move |_, t, _| {
                    let ds = Arc::clone(&ds);
                    t.spawn_link(tracing::Span::current(), |t| run(t, ds));
                    Ok(())
                }
            })
            .create_cap(t);
        let spec = from_io_value(&internal_services::DebtReporter)?;
        ds.assert(t, &Observe {
            pattern: syndicate_macros::pattern!{<require-service #(spec)>},
            observer: monitor,
        });
        Ok(())
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>) -> ActorResult {
    let spec = from_io_value(&internal_services::DebtReporter)?;
    ds.assert(t, syndicate_macros::template!("<service-running =spec>"));
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
