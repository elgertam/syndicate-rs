use std::iter::FromIterator;
use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::relay;
use syndicate::schemas::dataspace::Observe;
use syndicate::schemas::dataspace_patterns as p;
use syndicate::schemas::internal_protocol::*;
use syndicate::sturdy;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::Value;

use tokio::net::TcpStream;

use core::time::Duration;
use tokio::time::interval;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "d", default_value = "b4b303726566b10973796e646963617465b584b210a6480df5306611ddd0d3882b546e197784")]
    dataspace: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;
    Actor::new().boot(syndicate::name!("consumer"), |t| Box::pin(async move {
        let config = Config::from_args();
        let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
        let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
        relay::connect_stream(t, i, o, sturdyref, (), |_state, t, ds| {
            let consumer = syndicate::entity(0)
                .on_message(|message_count, _t, m| {
                    if m.value().is_boolean() {
                        tracing::info!("{:?} messages in the last second", message_count);
                        *message_count = 0;
                    } else {
                        *message_count += 1;
                    }
                    Ok(())
                })
                .create(t.actor);

            t.assert(&ds, &Observe {
                pattern: p::Pattern::DCompound(Box::new(p::DCompound::Rec {
                    ctor: Box::new(p::CRec {
                        label: Value::symbol("Says").wrap(),
                        arity: 2.into(),
                    }),
                    members: Map::from_iter(vec![
                        (0.into(), p::Pattern::DBind(Box::new(p::DBind {
                            name: "who".to_owned(),
                            pattern: p::Pattern::DDiscard(Box::new(p::DDiscard)),
                        }))),
                        (1.into(), p::Pattern::DBind(Box::new(p::DBind {
                            name: "what".to_owned(),
                            pattern: p::Pattern::DDiscard(Box::new(p::DDiscard)),
                        }))),
                    ].into_iter()),
                })),
                observer: Arc::clone(&consumer),
            });

            t.actor.linked_task(syndicate::name!("tick"), async move {
                let mut stats_timer = interval(Duration::from_secs(1));
                loop {
                    stats_timer.tick().await;
                    consumer.external_event(&Debtor::new(syndicate::name!("debtor")),
                                            Event::Message(Box::new(Message {
                                                body: Assertion(_Any::new(true)),
                                            }))).await?;
                }
            });
            Ok(None)
        });
        Ok(())
    })).await??;
    Ok(())
}
