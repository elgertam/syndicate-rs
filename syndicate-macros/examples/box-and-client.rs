use syndicate::actor::*;
use syndicate::enclose;
use syndicate::dataspace::Dataspace;
use syndicate::language;
use syndicate::schemas::dataspace::Observe;
use syndicate::value::NestedValue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;
    Actor::new().boot(tracing::Span::current(), |t| {
        let ds = Cap::new(&t.create(Dataspace::new()));
        let _ = t.prevent_inert_check();

        Actor::new().boot(syndicate::name!("box"), enclose!((ds) move |t| {
            let current_value = t.field(0u64);

            t.dataflow({
                let mut state_assertion_handle = None;
                enclose!((ds, current_value) move |t| {
                    let v = AnyValue::new(*t.get(&current_value));
                    tracing::info!(?v, "asserting");
                    ds.update(t, &mut state_assertion_handle, &(),
                              Some(&syndicate_macros::template!("<box-state =v>")));
                    Ok(())
                })
            })?;

            let set_box_handler = syndicate::entity(())
                .on_message(enclose!((current_value) move |(), t, captures: AnyValue| {
                    let v = captures.value().to_sequence()?[0].value().to_u64()?;
                    tracing::info!(?v, "from set-box");
                    t.set(&current_value, v);
                    Ok(())
                }))
                .create_cap(t);
            ds.assert(t, language(), &Observe {
                pattern: syndicate_macros::pattern!{<set-box $>},
                observer: set_box_handler,
            });

            t.dataflow(enclose!((current_value) move |t| {
                if *t.get(&current_value) == 1000000 {
                    t.stop();
                }
                Ok(())
            }))?;

            Ok(())
        }));

        Actor::new().boot(syndicate::name!("client"), enclose!((ds) move |t| {
            let box_state_handler = syndicate::entity(0u32)
                .on_asserted(enclose!((ds) move |count, t, captures: AnyValue| {
                    *count = *count + 1;
                    let value = captures.value().to_sequence()?[0].value().to_u64()?;
                    tracing::info!(?value);
                    let next = AnyValue::new(value + 1);
                    tracing::info!(?next, "sending");
                    ds.message(t, &(), &syndicate_macros::template!("<set-box =next>"));
                    Ok(Some(Box::new(|count, t| {
                        *count = *count - 1;
                        if *count == 0 {
                            tracing::info!("box state retracted");
                            t.stop();
                        }
                        Ok(())
                    })))
                }))
                .create_cap(t);
            ds.assert(t, language(), &Observe {
                pattern: syndicate_macros::pattern!{<box-state $>},
                observer: box_state_handler,
            });

            Ok(())
        }));

        Ok(())
    }).await??;
    Ok(())
}
