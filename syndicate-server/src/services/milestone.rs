use std::sync::Arc;

use syndicate::actor::*;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services::Milestone;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("milestone"), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: Milestone>, |t: &mut Activation| {
            tracing::info!(milestone = ?spec.name, "entered");
            ds.assert(t, language(), &lifecycle::started(&spec));
            ds.assert(t, language(), &lifecycle::ready(&spec));
            t.on_stop(move |_| { tracing::info!(milestone = ?spec.name, "exited"); Ok(()) });
            Ok(())
        }))
    });
}
