use syndicate::dataspace::Dataspace;
use syndicate::preserves::Set;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::rpc;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use syndicate::schemas::gatekeeper as G;
use syndicate::schemas::rpc as R;

use preserves_schema::Parse;
use preserves_schema::Unparse;

use syndicate::enclose;
use syndicate_macros::during;
use syndicate_macros::template;

pub fn start(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("path_resolver")), enclose!((ds) move |t| {
        during!(t, ds, <q <resolve-path $route0>>, |t| {
            if let Ok(route) = G::Route::parse(&route0) {
                let route_v = route.unparse();
                Supervisor::start(
                    t,
                    Some(template!("<path_resolver =route_v>")),
                    SupervisorConfiguration::default(),
                    |_t, _s| Ok(()),
                    enclose!((ds) move |t| enclose!((ds, route) run(t, ds, route))))
            } else {
                tracing::debug!(?route0, "Ignoring bogus route");
                Ok(())
            }
        });
        Ok(())
    }));

    t.spawn(Some(AnyValue::symbol("sturdy_ref_step")),
            enclose!((ds) move |t| super::sturdy::handle_sturdy_path_steps(t, ds)));

    t.spawn(Some(AnyValue::symbol("noise_ref_step")),
            enclose!((ds) move |t| super::noise::handle_noise_path_steps(t, ds)));
}

fn run(t: &mut Activation, ds: Arc<Cap>, route: G::Route) -> ActorResult {
    let candidates = t.named_field("candidates", Set::new());

    for addr in &route.transports {
        ds.assert(t, &rpc::question(G::ConnectTransport { addr: addr.clone() }));
        enclose!((candidates) during!(
            t, ds,
            <a <connect-transport #(addr)> <ok $c: G::ConnectedTransport::<Arc<Cap>>>>,
            |t: &mut Activation| {
                t.get_mut(&candidates).insert(c.clone());
                t.on_stop(enclose!((candidates, c) move |t: &mut Activation| {
                    t.get_mut(&candidates).remove(&c);
                    Ok(())
                }));
                Ok(())
            }));
    }

    let best = t.named_field("best", None);
    let root_peer = t.named_field("rootPeer", None);

    t.dataflow(enclose!((best, root_peer) move |t| {
        let c = t.get(&candidates).first().cloned();
        t.set(&root_peer, c.as_ref().map(
            |G::ConnectedTransport { responder_session, .. }| responder_session.clone()));
        t.set(&best, c);
        Ok(())
    }))?;

    let steps_ref = t.create(Dataspace::new(None));
    let steps_ds = Cap::new(&steps_ref);

    let mut handle_zero = None;
    t.dataflow(enclose!((root_peer) move |t| {
        let p = t.get(&root_peer).as_ref().cloned();
        t.update(&mut handle_zero, &steps_ref, p.map(|p| AnyValue::new(
            vec![AnyValue::new(0), AnyValue::embedded(p)])));
        Ok(())
    }))?;

    for (i, step) in route.path_steps.clone().into_iter().enumerate() {
        enclose!((ds, steps_ds) during!(
            t, steps_ds,
            [#(&AnyValue::new(i)), $origin: G::ResolvedPathStep::<Arc<Cap>>],
            enclose!((ds, step, steps_ds) move |t: &mut Activation| {
                let q = G::ResolvePathStep { origin: origin.0, path_step: step };
                ds.assert(t, &rpc::question(q.clone()));
                let q2 = q.clone();
                during!(
                    t, ds,
                    <a #(&q2.unparse()) $a>,
                    enclose!((q) |t| {
                        if let Ok(a) = R::Result::parse(&a) {
                            match a {
                                R::Result::Error(_) => {
                                    ds.assert(t, &rpc::answer(q, a));
                                }
                                R::Result::Ok(R::Ok { value }) => {
                                    if let Some(next) = value.as_embedded() {
                                        steps_ds.assert(t, &AnyValue::new(
                                            vec![AnyValue::new(i + 1),
                                                 AnyValue::embedded(next.into_owned())]));
                                    } else {
                                        ds.assert(t, &rpc::answer(
                                            q, R::Result::Error(R::Error {
                                                error: AnyValue::symbol("invalid-path-step-result"),
                                            })));
                                    }
                                }
                            }
                        }
                        Ok(())
                    }));
                Ok(())
            })));
    }

    let i = route.path_steps.len();
    during!(t, steps_ds,
            [#(&AnyValue::new(i)), $r: G::ResolvedPathStep::<Arc<Cap>>],
            enclose!((best, ds, route) move |t: &mut Activation| {
                let G::ConnectedTransport { addr, control, .. } =
                    t.get(&best).as_ref().unwrap().clone();
                let responder_session = r.0;
                ds.assert(t, &rpc::answer(
                    G::ResolvePath { route },
                    R::Result::Ok(R::Ok { value: (
                        G::ResolvedPath { addr, control, responder_session }).unparse() })));
                Ok(())
            }));

    Ok(())
}
