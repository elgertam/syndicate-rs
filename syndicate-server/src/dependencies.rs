use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::schemas::service;

use crate::counter;
use crate::language::language;

use syndicate_macros::during;
use syndicate_macros::template;

pub fn boot(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("dependencies_listener")), move |t| {
        Ok(during!(t, ds, language(), <require-service $spec>, |t: &mut Activation| {
            tracing::debug!(?spec, "tracking dependencies");
            let spec_v = language().unparse(&spec);
            t.spawn_link(Some(template!("<dependencies =spec_v>")),
                         enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, service_name: AnyValue) -> ActorResult {
    let obstacle_count = t.named_field("obstacle_count", 1isize);
    t.dataflow(enclose!((service_name, obstacle_count) move |t| {
        tracing::trace!(?service_name, obstacle_count = ?t.get(&obstacle_count));
        Ok(())
    }))?;

    t.dataflow({
        let mut handle = None;
        enclose!((ds, obstacle_count, service_name) move |t| {
            let obstacle_count = *t.get(&obstacle_count);
            if obstacle_count == 0 {
                ds.update(t, &mut handle, language(), Some(&service::RunService {
                    service_name: service_name.clone(),
                }));
            } else {
                ds.update::<_, service::RunService>(t, &mut handle, language(), None);
            }
            Ok(())
        })
    })?;

    let depender = service_name.clone();
    enclose!((ds, obstacle_count) during!(
        t, ds, language(), <depends-on #(&depender) $dependee>,
        enclose!((service_name, ds, obstacle_count) move |t: &mut Activation| {
            if let Ok(dependee) = language().parse::<service::ServiceState>(&dependee) {
                tracing::trace!(?service_name, ?dependee, "new dependency");
                ds.assert(t, language(), &service::RequireService {
                    service_name: dependee.service_name,
                });
            } else {
                tracing::warn!(?service_name, ?dependee, "cannot deduce dependee service name");
            }

            counter::adjust(t, &obstacle_count, 1);

            let d = &dependee.clone();
            during!(t, ds, language(), #d, enclose!(
                (service_name, obstacle_count, dependee) move |t: &mut Activation| {
                    tracing::trace!(?service_name, ?dependee, "dependency satisfied");
                    counter::adjust(t, &obstacle_count, -1);
                    Ok(())
                }));
            Ok(())
        })));

    counter::sync_and_adjust(t, &ds.underlying, &obstacle_count, -1);
    Ok(())
}
