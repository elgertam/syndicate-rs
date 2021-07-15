use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::relay;
use syndicate::sturdy;
use syndicate::value::NestedValue;
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
            let presence: _Any = Value::simple_record1(
                "Present",
                Value::from(std::process::id()).wrap()).wrap();

            let mut handle = Some(t.assert(&ds, presence.clone()));

            let producer = syndicate::entity(Arc::clone(&*INERT_REF))
                .on_message(move |self_ref, t, m| {
                    match m.value().to_boolean()? {
                        true => {
                            handle = Some(t.assert(&ds, presence.clone()));
                            t.message(&self_ref, _Any::new(false));
                        }
                        false => {
                            t.retract(handle.take().unwrap());
                            t.message(&self_ref, _Any::new(true));
                        }
                    }
                    Ok(())
                })
                .create_rec(t.actor, |_ac, self_ref, p_ref| *self_ref = Arc::clone(p_ref));

            t.message(&producer, _Any::new(false));
            Ok(None)
        });
        Ok(())
    })).await??;
    Ok(())
}
