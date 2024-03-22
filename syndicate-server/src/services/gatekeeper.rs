use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::preserves::rec;
use syndicate::preserves::value::NestedValue;

use crate::gatekeeper;
use crate::language::Language;
use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services::Gatekeeper;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("gatekeeper_listener")), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: Gatekeeper::<AnyValue>>, |t: &mut Activation| {
            t.spawn_link(Some(rec![AnyValue::symbol("gatekeeper"), language().unparse(&spec)]),
                         enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: Gatekeeper<AnyValue>) -> ActorResult {
    let resolver = t.create(syndicate::entity(Arc::clone(&spec.bindspace))
                            .on_asserted_facet(gatekeeper::facet_handle_resolve));
    ds.assert(t, language(), &syndicate::schemas::service::ServiceObject {
        service_name: language().unparse(&spec),
        object: AnyValue::domain(Cap::guard(Language::arc(), resolver)),
    });
    gatekeeper::handle_binds(t, &spec.bindspace)?;
    ds.assert(t, language(), &lifecycle::started(&spec));
    ds.assert(t, language(), &lifecycle::ready(&spec));
    Ok(())
}
