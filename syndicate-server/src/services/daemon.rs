use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::value::NestedValue;

use crate::language::language;
use crate::schemas::external_services::DaemonService;

use syndicate_macros::during;

// use syndicate::schemas::dataspace_patterns::*;
// impl DaemonService {
//     fn wildcard_dataspace_pattern() -> Pattern {
//         Pattern::DDiscard(Box::new(DDiscard))
//     }
// }

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {

        during!(t, config_ds, language(), <require-service $spec: DaemonService>, |t| {
            let config_ds = Arc::clone(&config_ds);
            let root_ds = Arc::clone(&root_ds);
            Ok(Supervisor::start(
                t,
                syndicate::name!(parent: None, "daemon", service = ?spec),
                SupervisorConfiguration::default(),
                move |t| run(t, Arc::clone(&config_ds), Arc::clone(&root_ds), spec.clone())))
        });

        Ok(())
    });
}

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    _root_ds: Arc<Cap>,
    captures: DaemonService,
) -> ActorResult {
    {
        let spec = language().unparse(&captures);
        config_ds.assert(t, &(), &syndicate_macros::template!("<service-running =spec>"));
    }

    tracing::info!("daemon {:?}", &captures);

    Ok(())
}
