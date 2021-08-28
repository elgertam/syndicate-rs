use std::sync::Arc;

use syndicate::actor::*;
use syndicate::during::entity;
use syndicate::schemas::dataspace::Observe;
use syndicate::value::NestedValue;

const SERVICE_NAME: &str = "debt-reporter";

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", service = SERVICE_NAME), move |t| {
        let monitor = entity(())
            .on_asserted_facet(|_, t, _| {
                t.spawn_link(syndicate::name!(SERVICE_NAME), run);
                Ok(())
            })
            .create_cap(t);
        let service_sym = AnyValue::symbol(SERVICE_NAME);
        ds.assert(t, &Observe {
            pattern: syndicate_macros::pattern!("<require-service =service_sym>"),
            observer: monitor,
        });
        Ok(())
    })
}

// pub fn spawn(t: &mut Activation) {
//     t.spawn(syndicate::name!(SERVICE_NAME), run);
// }

fn run(t: &mut Activation) -> ActorResult {
    t.linked_task(syndicate::name!("tick"), async {
        let mut timer = tokio::time::interval(core::time::Duration::from_secs(1));
        loop {
            timer.tick().await;
            for (id, (name, debt)) in syndicate::actor::ACCOUNTS.read().unwrap().iter() {
                let _enter = name.enter();
                tracing::info!(id, debt = debug(
                    debt.load(std::sync::atomic::Ordering::Relaxed)));
            }
        }
    });
    Ok(())
}
