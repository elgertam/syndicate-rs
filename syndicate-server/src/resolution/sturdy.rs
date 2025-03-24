use std::sync::Arc;

use syndicate::actor::*;
use syndicate::rpc;

use syndicate::enclose;
use syndicate_macros::during;
use syndicate_macros::pattern;

use syndicate::preserves::ExpectedKind;

use syndicate::schemas::dataspace;
use syndicate::schemas::gatekeeper;
use syndicate::schemas::sturdy;
use syndicate::schemas::rpc as R;

use preserves_schema::Parse;
use preserves_schema::Unparse;
use preserves_schema::unparse_iovalue;

fn sturdy_step_type() -> String {
    unparse_iovalue(&sturdy::SturdyStepType).to_symbol().unwrap().into_owned()
}

pub fn handle_sturdy_binds(t: &mut Activation, ds: &Arc<Cap>) -> ActorResult {
    during!(t, ds, <bind <ref $desc> $target $observer>, |t: &mut Activation| {
        t.spawn_link(None, move |t| {
            target.to_embedded()?;
            let observer = gatekeeper::BindObserver::parse(&observer)?;
            let desc = sturdy::SturdyDescriptionDetail::parse(&desc)?;
            let sr = sturdy::SturdyRef::mint(desc.oid, &desc.key);
            if let gatekeeper::BindObserver::Present(o) = observer {
                o.assert(t, &gatekeeper::Bound::Bound {
                    path_step: gatekeeper::PathStep {
                        step_type: sturdy_step_type(),
                        detail: sr.parameters.unparse(),
                    },
                });
            }
            Ok(())
        });
        Ok(())
    });
    Ok(())
}

pub fn take_sturdy_step(t: &mut Activation, ds: &mut Arc<Cap>, a: &gatekeeper::Resolve, detail: &mut &'static str) -> Result<bool, ActorError> {
    if a.step.step_type == sturdy_step_type() {
        *detail = "invalid";
        if let Ok(s) = sturdy::SturdyStepDetail::parse(&a.step.detail) {
            t.facet(|t| {
                let f = super::handle_direct_resolution(ds, t, a.clone())?;
                await_bind_sturdyref(ds, t, sturdy::SturdyRef { parameters: s.0 }, a.observer.clone(), f)
            })?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn await_bind_sturdyref(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    sturdyref: sturdy::SturdyRef,
    observer: Arc<Cap>,
    direct_resolution_facet: FacetId,
) -> ActorResult {
    let queried_oid = sturdyref.parameters.oid.clone();
    let handler = syndicate::entity(observer)
        .on_asserted(move |observer, t, a: AnyValue| {
            t.stop_facet(direct_resolution_facet);
            let bindings = a.collect_sequence().ok_or(ExpectedKind::Sequence)?;
            let key = bindings[0].to_bytestring()?;
            let unattenuated_target = bindings[1].to_embedded()?;
            match sturdyref.validate_and_attenuate(key.as_ref(), unattenuated_target.as_ref()) {
                Err(e) => {
                    tracing::warn!(sturdyref = ?sturdyref.unparse(),
                                   "sturdyref failed validation: {}", e);
                    observer.assert(t, &gatekeeper::Resolved::Rejected(
                        gatekeeper::Rejected {
                            detail: AnyValue::symbol("sturdyref-failed-validation"),
                        }));
                },
                Ok(target) => {
                    tracing::trace!(sturdyref = ?sturdyref.unparse(),
                                    ?target,
                                    "sturdyref resolved");
                    observer.assert(t, &gatekeeper::Resolved::Accepted {
                        responder_session: target,
                    });
                }
            }
            Ok(None)
        })
        .create_cap(t);
    ds.assert(t, &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: pattern!{<bind <ref { oid: #(&queried_oid), key: $ }> $ _>},
        observer: handler,
    });
    Ok(())
}

pub fn handle_sturdy_path_steps(t: &mut Activation, ds: Arc<Cap>) -> ActorResult {
    during!(t, ds,
            <q <resolve-path-step $origin <ref $parameters: sturdy::SturdyPathStepDetail::<Arc<Cap>>>>>,
            enclose!((ds) move |t: &mut Activation| {
                if let Some(origin) = origin.as_embedded().map(|r| r.into_owned()) {
                    let observer = Cap::guard(t.create(
                        syndicate::entity(()).on_asserted_facet(
                            enclose!((origin, parameters) move |_, t, r: gatekeeper::Resolved| {
                                ds.assert(t, &rpc::answer(
                                    gatekeeper::ResolvePathStep {
                                        origin: origin.clone(),
                                        path_step: gatekeeper::PathStep {
                                            step_type: "ref".to_string(),
                                            detail: parameters.unparse(),
                                        },
                                    },
                                    match r {
                                        gatekeeper::Resolved::Accepted { responder_session } =>
                                            R::Result::Ok { value: (
                                                &gatekeeper::ResolvedPathStep(responder_session)).unparse() },
                                        gatekeeper::Resolved::Rejected(b) =>
                                            R::Result::Error { error: b.detail },
                                    }));
                                Ok(())
                            }))));
                    origin.assert(t, &gatekeeper::Resolve {
                        step: gatekeeper::Step {
                            step_type: "ref".to_string(),
                            detail: parameters.unparse(),
                        },
                        observer,
                    });
                }
                Ok(())
            }));
    Ok(())
}
