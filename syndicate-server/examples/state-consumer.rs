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
use tokio::time::interval;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "d", default_value = "b4b303726566b10973796e646963617465b584b210a6480df5306611ddd0d3882b546e197784")]
    dataspace: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;
    let config = Config::from_args();
    let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
    let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
    Actor::new().boot(syndicate::name!("state-consumer"), |t| {
        relay::connect_stream(t, i, o, sturdyref, (), |_state, t, ds| {
            let consumer = {
                #[derive(Default)]
                struct State {
                    event_counter: u64,
                    arrival_counter: u64,
                    departure_counter: u64,
                    occupancy: u64,
                }
                syndicate::entity(State::default()).on_asserted(move |s, _, _| {
                    s.event_counter += 1;
                    s.arrival_counter += 1;
                    s.occupancy += 1;
                    Ok(Some(Box::new(|s, _| {
                        s.event_counter += 1;
                        s.departure_counter += 1;
                        s.occupancy -= 1;
                        Ok(())
                    })))
                }).on_message(move |s, _, _| {
                    tracing::info!(
                        "{:?} events, {:?} arrivals, {:?} departures, {:?} present in the last second",
                        s.event_counter,
                        s.arrival_counter,
                        s.departure_counter,
                        s.occupancy);
                    s.event_counter = 0;
                    s.arrival_counter = 0;
                    s.departure_counter = 0;
                    Ok(())
                }).create_cap(t)
            };

            ds.assert(t, language(), &Observe {
                pattern: syndicate_macros::pattern!{<Present $>},
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
    }).await??;
    Ok(())
}
