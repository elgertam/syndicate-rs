use std::sync::Arc;
use syndicate::actor::*;
use syndicate::schemas::gatekeeper;

use syndicate::enclose;

use crate::language;

pub mod sturdy;
pub mod noise;

fn handle_direct_resolution(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: gatekeeper::Resolve,
) -> Result<FacetId, ActorError> {
    let outer_facet = t.facet_id();
    t.facet(move |t| {
        let handler = syndicate::entity(a.observer)
            .on_asserted(move |observer, t, a: AnyValue| {
                t.stop_facet_and_continue(outer_facet, Some(
                    enclose!((observer, a) move |t: &mut Activation| {
                        observer.assert(t, language(), &a);
                        Ok(())
                    })))?;
                Ok(None)
            })
            .create_cap(t);
        ds.assert(t, language(), &gatekeeper::Resolve {
            step: a.step.clone(),
            observer: handler,
        });
        Ok(())
    })
}
