use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::during::entity;
use syndicate::enclose;
use syndicate::schemas::dataspace::Observe;
use syndicate::schemas::service;
use syndicate::value::NestedValue;

use crate::counter;
use crate::language::language;
use crate::schemas::internal_services;

use syndicate_macros::during;

pub fn boot(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("tracker", module = module_path!()), move |t| {
        Ok(during!(t, ds, language(), <require-service $spec>, |t: &mut Activation| {
            tracing::info!(?spec, "tracking dependencies");
            t.spawn_link(syndicate::name!(parent: None, "tracker", spec = ?spec),
                         enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, service_name: AnyValue) -> ActorResult {
    if !service_name.value().is_simple_record("milestone", Some(1)) {
        let core_dep = service::ServiceDependency {
            depender: service_name.clone(),
            dependee: service::ServiceState {
                service_name: language().unparse(&internal_services::Milestone {
                    name: AnyValue::symbol("core"),
                }),
                state: service::State::Ready,
            },
        };
        let milestone_monitor = entity(ds.assert(t, language(), &core_dep))
            .on_asserted(enclose!((ds) move |handle, t, _captures: AnyValue| {
                ds.update::<_, service::ServiceDependency>(t, handle, language(), None);
                Ok(Some(Box::new(enclose!((ds, core_dep) move |handle, t| {
                    ds.update(t, handle, language(), Some(&core_dep));
                    Ok(())
                }))))
            }))
            .create_cap(t);
        ds.assert(t, language(), &Observe {
            pattern: syndicate_macros::pattern!{<core-service #(service_name.clone())>},
            observer: milestone_monitor,
        });
    }

    let obstacle_count = t.field(1isize);
    t.dataflow(enclose!((obstacle_count) move |t| {
        tracing::trace!(obstacle_count = ?t.get(&obstacle_count));
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

    enclose!((ds, obstacle_count) during!(
        t, ds, language(), <depends-on #(service_name.clone()) $dependee>,
        enclose!((ds, obstacle_count) move |t: &mut Activation| {
            if let Ok(dependee) = language().parse::<service::ServiceState>(&dependee) {
                tracing::trace!(on = ?dependee, "new dependency");
                ds.assert(t, language(), &service::RequireService {
                    service_name: dependee.service_name,
                });
            } else {
                tracing::warn!(on = ?dependee, "cannot deduce dependee service name");
            }

            counter::adjust(t, &obstacle_count, 1);

            let d = dependee.clone();
            during!(t, ds, language(), #(d), enclose!(
                (obstacle_count, dependee) move |t: &mut Activation| {
                    tracing::trace!(on = ?dependee, "dependency satisfied");
                    counter::adjust(t, &obstacle_count, -1);
                    Ok(())
                }));
            Ok(())
        })));

    counter::sync_and_adjust(t, &ds.underlying, &obstacle_count, -1);
    Ok(())
}
