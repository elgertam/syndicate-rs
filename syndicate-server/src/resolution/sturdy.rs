use std::sync::Arc;

use preserves_schema::Codec;

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

use crate::language;

fn sturdy_step_type() -> String {
    language().unparse(&sturdy::SturdyStepType).to_symbol().unwrap().into_owned()
}

pub fn handle_sturdy_binds(t: &mut Activation, ds: &Arc<Cap>) -> ActorResult {
    during!(t, ds, language(), <bind <ref $desc> $target $observer>, |t: &mut Activation| {
        t.spawn_link(None, move |t| {
            target.to_embedded()?;
            let observer = language().parse::<gatekeeper::BindObserver>(&observer)?;
            let desc = language().parse::<sturdy::SturdyDescriptionDetail>(&desc)?;
            let sr = sturdy::SturdyRef::mint(desc.oid, &desc.key);
            if let gatekeeper::BindObserver::Present(o) = observer {
                o.assert(t, language(), &gatekeeper::Bound::Bound {
                    path_step: gatekeeper::PathStep {
                        step_type: sturdy_step_type(),
                        detail: language().unparse(&sr.parameters),
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
        if let Ok(s) = language().parse::<sturdy::SturdyStepDetail>(&a.step.detail) {
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
                    tracing::warn!(sturdyref = ?language().unparse(&sturdyref),
                                   "sturdyref failed validation: {}", e);
                    observer.assert(t, language(), &gatekeeper::Resolved::Rejected(
                        gatekeeper::Rejected {
                            detail: AnyValue::symbol("sturdyref-failed-validation"),
                        }));
                },
                Ok(target) => {
                    tracing::trace!(sturdyref = ?language().unparse(&sturdyref),
                                    ?target,
                                    "sturdyref resolved");
                    observer.assert(t, language(), &gatekeeper::Resolved::Accepted {
                        responder_session: target,
                    });
                }
            }
            Ok(None)
        })
        .create_cap(t);
    ds.assert(t, language(), &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: pattern!{<bind <ref { oid: #(&queried_oid), key: $ }> $ _>},
        observer: handler,
    });
    Ok(())
}

pub fn handle_sturdy_path_steps(t: &mut Activation, ds: Arc<Cap>) -> ActorResult {
    during!(t, ds, language(),
            <q <resolve-path-step $origin <ref $parameters: sturdy::SturdyPathStepDetail::<Arc<Cap>>>>>,
            enclose!((ds) move |t: &mut Activation| {
                if let Some(origin) = origin.as_embedded().map(|r| r.into_owned()) {
                    let observer = Cap::guard(&language().syndicate, t.create(
                        syndicate::entity(()).on_asserted_facet(
                            enclose!((origin, parameters) move |_, t, r: gatekeeper::Resolved| {
                                ds.assert(t, language(), &rpc::answer(
                                    language(),
                                    gatekeeper::ResolvePathStep {
                                        origin: origin.clone(),
                                        path_step: gatekeeper::PathStep {
                                            step_type: "ref".to_string(),
                                            detail: language().unparse(&parameters),
                                        },
                                    },
                                    match r {
                                        gatekeeper::Resolved::Accepted { responder_session } =>
                                            R::Result::Ok { value: language().unparse(
                                                &gatekeeper::ResolvedPathStep(responder_session)) },
                                        gatekeeper::Resolved::Rejected(b) =>
                                            R::Result::Error { error: b.detail },
                                    }));
                                Ok(())
                            }))));
                    origin.assert(t, language(), &gatekeeper::Resolve {
                        step: gatekeeper::Step {
                            step_type: "ref".to_string(),
                            detail: language().unparse(&parameters),
                        },
                        observer,
                    });
                }
                Ok(())
            }));
    Ok(())
}
