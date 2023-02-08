use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::language;
use syndicate::relay;
use syndicate::schemas::dataspace::Observe;
use syndicate::sturdy;
use syndicate::value::NestedValue;

use tokio::net::TcpStream;

use core::time::Duration;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "d", default_value = "b4b303726566b7b3036f6964b10973796e646963617465b303736967b21069ca300c1dbfa08fba692102dd82311a8484")]
    dataspace: String,
}

#[tokio::main]
async fn main() -> ActorResult {
    syndicate::convenient_logging()?;
    let config = Config::from_args();
    let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
    let (i, o) = TcpStream::connect("127.0.0.1:9001").await?.into_split();
    Actor::top(None, |t| {
        relay::connect_stream(t, i, o, false, sturdyref, (), |_state, t, ds| {
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
            ds.assert(t, language(), &Observe {
                pattern: syndicate_macros::pattern!{<Says $ $>},
                observer: Arc::clone(&consumer),
            });

            t.every(Duration::from_secs(1), move |t| {
                consumer.message(t, &(), &AnyValue::new(true));
                Ok(())
            })?;

            Ok(None)
        })
    }).await??;
    Ok(())
}
