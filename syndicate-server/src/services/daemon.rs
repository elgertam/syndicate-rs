use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use crate::language::language;
use crate::schemas::external_services::{DaemonService, DaemonSpec};

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, config_ds, language(), <require-service $spec: DaemonService>, |t| {
            let config_ds = Arc::clone(&config_ds);
            let root_ds = Arc::clone(&root_ds);
            Ok(Supervisor::start(
                t,
                syndicate::name!(parent: None, "daemon", service = ?spec),
                SupervisorConfiguration::default(),
                move |t| run(t, Arc::clone(&config_ds), Arc::clone(&root_ds), spec.clone())))
        }))
    });
}

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    _root_ds: Arc<Cap>,
    service: DaemonService,
) -> ActorResult {
    {
        let spec = language().unparse(&service);
        config_ds.assert(t, &(), &syndicate_macros::template!("<service-running =spec>"));
    }

    let id = service.id.0.clone();

    Ok(during!(t, config_ds, language(), <daemon #(id) $config>, |_t| {
        if let Ok(config) = language().parse::<DaemonSpec>(&config) {
            tracing::info!("daemon {:?} {:?}", &service, &config);
        }
        Ok(())
    }))
}
