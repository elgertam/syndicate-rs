use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::relay;
use syndicate::schemas::dataspace::Observe;
use syndicate::sturdy;
use syndicate::value::NestedValue;

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
    Actor::new().boot(syndicate::name!("consumer"), |t| {
        let facet = t.facet.clone();
        let boot_account = Arc::clone(t.account());
        Ok(t.linked_task(tracing::Span::current(), async move {
            let config = Config::from_args();
            let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
            let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
            facet.activate(boot_account, |t| {
                relay::connect_stream(t, i, o, sturdyref, (), |_state, t, ds| {
                    let consumer = syndicate::entity(0)
                        .on_message(|message_count, _t, m: AnyValue| {
                            if m.value().is_boolean() {
                                tracing::info!("{:?} messages in the last second", message_count);
                                *message_count = 0;
                            } else {
                                *message_count += 1;
                            }
                            Ok(())
                        })
                        .create_cap(t);
                    ds.assert(t, &Observe {
                        pattern: syndicate_macros::pattern!("<Says $ $>"),
                        observer: Arc::clone(&consumer),
                    });

                    t.linked_task(syndicate::name!("tick"), async move {
                        let mut stats_timer = interval(Duration::from_secs(1));
                        loop {
                            stats_timer.tick().await;
                            let consumer = Arc::clone(&consumer);
                            external_event(&Arc::clone(&consumer.underlying.mailbox),
                                           &Account::new(syndicate::name!("account")),
                                           Box::new(move |t| t.with_entity(
                                               &consumer.underlying,
                                               |t, e| e.message(t, AnyValue::new(true)))))?;
                        }
                    });
                    Ok(None)
                });
                Ok(())
            })
        }))
    }).await??;
    Ok(())
}
