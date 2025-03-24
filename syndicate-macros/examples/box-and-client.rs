use syndicate::actor::*;
use syndicate::enclose;
use syndicate::dataspace::Dataspace;
use syndicate::schemas::dataspace::Observe;

#[tokio::main]
async fn main() -> ActorResult {
    syndicate::convenient_logging()?;
    Actor::top(None, |t| {
        let ds = Cap::new(&t.create(Dataspace::new(None)));
        let _ = t.prevent_inert_check();

        t.spawn(Some(AnyValue::symbol("box")), enclose!((ds) move |t| {
            let current_value = t.named_field("current_value", 0u64);

            t.dataflow({
                let mut state_assertion_handle = None;
                enclose!((ds, current_value) move |t| {
                    let v = AnyValue::new(*t.get(&current_value));
                    tracing::info!(?v, "asserting");
                    ds.update(t, &mut state_assertion_handle,
                              Some(&syndicate_macros::template!("<box-state =v>")));
                    Ok(())
                })
            })?;

            let set_box_handler = syndicate::entity(())
                .on_message(enclose!((current_value) move |(), t, captures: AnyValue| {
                    if let Some(v) = captures.to_sequence()?.nth(0) {
                        let v = v.to_u64()??;
                        tracing::info!(?v, "from set-box");
                        t.set(&current_value, v);
                    }
                    Ok(())
                }))
                .create_cap(t);
            ds.assert(t, &Observe {
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

        t.spawn(Some(AnyValue::symbol("client")), enclose!((ds) move |t| {
            let box_state_handler = syndicate::entity(0u32)
                .on_asserted(enclose!((ds) move |count, t, captures: AnyValue| {
                    *count = *count + 1;
                    if let Some(value) = captures.to_sequence()?.nth(0) {
                        let value = value.to_u64()??;
                        tracing::info!(?value);
                        let next = AnyValue::new(value + 1);
                        tracing::info!(?next, "sending");
                        ds.message(t, &syndicate_macros::template!("<set-box =next>"));
                        Ok(Some(Box::new(|count, t| {
                            *count = *count - 1;
                            if *count == 0 {
                                tracing::info!("box state retracted");
                                t.stop();
                            }
                            Ok(())
                        })))
                    } else {
                        Ok(None)
                    }
                }))
                .create_cap(t);
            ds.assert(t, &Observe {
                pattern: syndicate_macros::pattern!{<box-state $>},
                observer: box_state_handler,
            });

            Ok(())
        }));

        Ok(())
    }).await??;
    Ok(())
}
