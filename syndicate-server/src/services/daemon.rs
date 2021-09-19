use std::convert::TryFrom;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::during::entity;
use syndicate::schemas::dataspace::Observe;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::value::NestedValue;

use crate::schemas::external_services;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    // t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {

    //     during!(t, config_ds, <require-service $spec: external_services::DaemonService>, |t| {
    //         let config_ds = Arc::clone(&config_ds);
    //         let root_ds = Arc::clone(&root_ds);
    //         Ok(Supervisor::start(
    //             t,
    //             syndicate::name!(parent: None, "daemon", service = ?spec_any),
    //             SupervisorConfiguration::default(),
    //             move |t| run(t, Arc::clone(&config_ds), Arc::clone(&root_ds), spec.clone())))
    //     });

    //     Ok(())
    // });
}

// fn run(
//     t: &mut Activation,
//     config_ds: Arc<Cap>,
//     _root_ds: Arc<Cap>,
//     captures: AnyValue,
// ) -> ActorResult {
//     let spec = external_services::DaemonService::try_from(&from_any_value(
//         &captures.value().to_sequence()?[0])?)?;
//     {
//         let spec = from_io_value(&spec)?;
//         config_ds.assert(t, syndicate_macros::template!("<service-running =spec>"));
//     }

//     tracing::info!("daemon {:?}", &spec);

//     Ok(())
// }
