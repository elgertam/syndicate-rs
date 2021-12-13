use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::during::DuringResult;
use syndicate::schemas::gatekeeper;
use syndicate::value::NestedValue;

use crate::language::language;

// pub fn bind(
//     t: &mut Activation,
//     ds: &Arc<Cap>,
//     oid: syndicate::schemas::sturdy::_Any,
//     key: [u8; 16],
//     target: Arc<Cap>,
// ) {
//     let sr = sturdy::SturdyRef::mint(oid.clone(), &key);
//     tracing::info!(cap = ?language().unparse(&sr), hex = %sr.to_hex());
//     ds.assert(t, language(), &gatekeeper::Bind { oid, key: key.to_vec(), target });
// }

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
                    tracing::warn!(sturdyref = ?language().unparse(&sturdyref),
                                   "sturdyref failed validation: {}", e);
                    Ok(None)
                },
                Ok(target) => {
                    tracing::trace!(sturdyref = ?language().unparse(&sturdyref),
                                    ?target,
                                    "sturdyref resolved");
                    if let Some(h) = observer.assert(t, &(), &AnyValue::domain(target)) {
                        Ok(Some(Box::new(move |_observer, t| Ok(t.retract(h)))))
                    } else {
                        Ok(None)
                    }
                }
            }
        })
        .create_cap(t);
    if let Some(oh) = ds.assert(t, language(), &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: syndicate_macros::pattern!{<bind #(&queried_oid) $ $>},
        observer: handler,
    }) {
        Ok(Some(Box::new(move |_ds, t| Ok(t.retract(oh)))))
    } else {
        Ok(None)
    }
}
