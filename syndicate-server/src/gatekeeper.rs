use std::sync::Arc;

use syndicate::actor::*;
use syndicate::during::DuringResult;
use syndicate::schemas::gatekeeper;
use syndicate::value::NestedValue;

pub fn handle_resolve(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: gatekeeper::Resolve,
) -> DuringResult<Arc<Cap>> {
    use syndicate::schemas::dataspace;

    let gatekeeper::Resolve { sturdyref, observer } = a;
    let queried_oid = sturdyref.oid.clone();
    let handler = syndicate::entity(observer)
        .on_asserted(move |observer, t, a: AnyValue| {
            let bindings = a.value().to_sequence()?;
            let key = bindings[0].value().to_bytestring()?;
            let unattenuated_target = bindings[1].value().to_embedded()?;
            match sturdyref.validate_and_attenuate(key, unattenuated_target) {
                Err(e) => {
                    tracing::warn!(sturdyref = debug(&AnyValue::from(&sturdyref)),
                                   "sturdyref failed validation: {}", e);
                    Ok(None)
                },
                Ok(target) => {
                    tracing::trace!(sturdyref = debug(&AnyValue::from(&sturdyref)),
                                    target = debug(&target),
                                    "sturdyref resolved");
                    if let Some(h) = observer.assert(t, AnyValue::domain(target)) {
                        Ok(Some(Box::new(move |_observer, t| Ok(t.retract(h)))))
                    } else {
                        Ok(None)
                    }
                }
            }
        })
        .create_cap(t);
    if let Some(oh) = ds.assert(t, &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: syndicate_macros::pattern!("<bind =queried_oid $ $>"),
        observer: handler,
    }) {
        Ok(Some(Box::new(move |_ds, t| Ok(t.retract(oh)))))
    } else {
        Ok(None)
    }
}
