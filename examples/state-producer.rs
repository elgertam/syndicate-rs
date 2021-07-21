use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::relay;
use syndicate::schemas::internal_protocol::*;
use syndicate::sturdy;
use syndicate::value::Value;

use tokio::net::TcpStream;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "d", default_value = "b4b303726566b10973796e646963617465b584b210a6480df5306611ddd0d3882b546e197784")]
    dataspace: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;
    Actor::new().boot(syndicate::name!("producer"), |t| Box::pin(async move {
        let config = Config::from_args();
        let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
        let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
        relay::connect_stream(t, i, o, sturdyref, (), move |_state, t, ds| {
            let debtor = Debtor::new(syndicate::name!("debtor"));
            t.actor.linked_task(syndicate::name!("sender"), async move {
                let presence: _Any = Value::simple_record1(
                    "Present",
                    Value::from(std::process::id()).wrap()).wrap();
                let handle = syndicate::next_handle();
                let assert_e = Event::Assert(Box::new(Assert {
                    assertion: Assertion(presence),
                    handle: handle.clone(),
                }));
                let retract_e = Event::Retract(Box::new(Retract {
                    handle,
                }));
                external_event(&ds, &debtor, assert_e.clone()).await?;
                loop {
                    debtor.ensure_clear_funds().await;
                    external_event(&ds, &debtor, retract_e.clone()).await?;
                    external_event(&ds, &debtor, assert_e.clone()).await?;
                }
            });
            Ok(None)
        });
        Ok(())
    })).await??;
    Ok(())
}
