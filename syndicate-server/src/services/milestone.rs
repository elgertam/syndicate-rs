use std::sync::Arc;

use syndicate::actor::*;
use syndicate::preserves_schema::Codec;
use syndicate::schemas::service;

use crate::language::language;
use crate::schemas::internal_services::Milestone;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: Milestone>, |t: &mut Activation| {
            ds.assert(t, language(), &service::ServiceRunning {
                service_name: language().unparse(&spec),
            });
            Ok(())
        }))
    });
}
