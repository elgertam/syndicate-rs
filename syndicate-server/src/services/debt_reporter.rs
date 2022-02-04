use preserves_schema::Codec;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::preserves::rec;
use syndicate::preserves::value::NestedValue;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services::DebtReporter;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("debt_reporter_listener")), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: DebtReporter>, |t: &mut Activation| {
            t.spawn_link(Some(rec![AnyValue::symbol("debt_reporter"), language().unparse(&spec)]),
                         enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: DebtReporter) -> ActorResult {
    ds.assert(t, language(), &lifecycle::started(&spec));
    ds.assert(t, language(), &lifecycle::ready(&spec));
    t.every(core::time::Duration::from_millis((spec.interval_seconds.0 * 1000.0) as u64), |_t| {
        for (account_id, (name, debt)) in syndicate::actor::ACCOUNTS.read().iter() {
            tracing::info!(account_id, ?name, debt = ?debt.load(Ordering::Relaxed));
        }

        // let snapshot = syndicate::actor::ACTORS.read().clone();
        // for (id, (name, ac_ref)) in snapshot.iter() {
        //     if *id == _t.state.actor_id {
        //         tracing::debug!("skipping report on the reporting actor, to avoid deadlock");
        //         continue;
        //     }
        //     tracing::trace!(?id, "about to lock");
        //     tracing::info_span!("actor", id, ?name).in_scope(|| match &*ac_ref.state.lock() {
        //         ActorState::Terminated { exit_status } =>
        //             tracing::info!(?exit_status, "terminated"),
        //         ActorState::Running(state) => {
        //             tracing::info!(field_count = ?state.fields.len(),
        //                            outbound_assertion_count = ?state.outbound_assertions.len(),
        //                            facet_count = ?state.facet_nodes.len());
        //             tracing::info_span!("facets").in_scope(|| {
        //                 for (facet_id, f) in state.facet_nodes.iter() {
        //                     tracing::info!(
        //                         ?facet_id,
        //                         parent_id = ?f.parent_facet_id,
        //                         outbound_handle_count = ?f.outbound_handles.len(),
        //                         linked_task_count = ?f.linked_tasks.len(),
        //                         inert_check_preventers = ?f.inert_check_preventers.load(Ordering::Relaxed));
        //                 }
        //             });
        //         }
        //     });
        // }

        Ok(())
    })
}
