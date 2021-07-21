use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::relay;
use syndicate::schemas::internal_protocol::*;
use syndicate::sturdy;
use syndicate::value::Value;

use tokio::net::TcpStream;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "a", default_value = "1")]
    action_count: u32,

    #[structopt(short = "b", default_value = "0")]
    bytes_padding: usize,

    #[structopt(short = "d", default_value = "b4b303726566b10973796e646963617465b584b210a6480df5306611ddd0d3882b546e197784")]
    dataspace: String,
}

#[inline]
fn says(who: _Any, what: _Any) -> _Any {
    let mut r = Value::simple_record("Says", 2);
    r.fields_vec_mut().push(who);
    r.fields_vec_mut().push(what);
    r.finish().wrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;
    syndicate::actor::start_debt_reporter();
    Actor::new().boot(syndicate::name!("producer"), |t| Box::pin(async move {
        let config = Config::from_args();
        let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
        let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
        relay::connect_stream(t, i, o, sturdyref, (), move |_state, t, ds| {
            let padding: _Any = Value::ByteString(vec![0; config.bytes_padding]).wrap();
            let action_count = config.action_count;
            let debtor = Debtor::new(syndicate::name!("debtor"));
            t.actor.linked_task(syndicate::name!("sender"), async move {
                loop {
                    debtor.ensure_clear_funds().await;
                    let mut events = Vec::new();
                    for _ in 0..action_count {
                        events.push((ds.clone(), Event::Message(Box::new(Message {
                            body: Assertion(says(Value::from("producer").wrap(), padding.clone())),
                        }))));
                    }
                    external_events(&ds, &debtor, events).await?;
                }
            });
            Ok(None)
        });
        Ok(())
    })).await??;
    Ok(())
}
