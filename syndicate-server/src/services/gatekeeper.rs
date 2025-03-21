use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::schemas::gatekeeper;

use syndicate_macros::during;
use syndicate_macros::template;

use crate::language::Language;
use crate::language::language;
use crate::lifecycle;
use crate::resolution::sturdy;
use crate::resolution::noise;
use crate::schemas::internal_services::Gatekeeper;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("gatekeeper_listener")), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: Gatekeeper::<Arc<Cap>>>, |t: &mut Activation| {
            let spec_v = language().unparse(&spec);
            t.spawn_link(Some(template!("<gatekeeper =spec_v>")), enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

pub fn create_gatekeeper(t: &mut Activation, bindspace: &Arc<Cap>) -> Result<Arc<Cap>, ActorError> {
    sturdy::handle_sturdy_binds(t, bindspace)?;
    noise::handle_noise_binds(t, bindspace)?;
    Ok(Cap::guard(Language::arc(), t.create(
        syndicate::entity(Arc::clone(bindspace))
            .on_asserted_facet(facet_handle_resolve))))
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: Gatekeeper<Arc<Cap>>) -> ActorResult {
    let gk = create_gatekeeper(t, &spec.bindspace)?;
    ds.assert(t, language(), &syndicate::schemas::service::ServiceObject {
        service_name: language().unparse(&spec),
        object: AnyValue::embedded(gk),
    });
    ds.assert(t, language(), &lifecycle::started(&spec));
    ds.assert(t, language(), &lifecycle::ready(&spec));
    Ok(())
}

fn facet_handle_resolve(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: gatekeeper::Resolve,
) -> ActorResult {
    let mut detail: &'static str = "unsupported";
    if sturdy::take_sturdy_step(t, ds, &a, &mut detail)? { return Ok(()); }
    if noise::take_noise_step(t, ds, &a, &mut detail)? { return Ok(()); }
    a.observer.assert(t, language(), &gatekeeper::Rejected {
        detail: AnyValue::symbol(detail),
    });
    Ok(())
}
