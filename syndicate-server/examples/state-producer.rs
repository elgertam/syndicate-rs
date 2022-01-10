use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::relay;
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
    let config = Config::from_args();
    let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
    let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
    Actor::new(None).boot(syndicate::name!("state-producer"), |t| {
        relay::connect_stream(t, i, o, false, sturdyref, (), move |_state, t, ds| {
            let account = Account::new(syndicate::name!("account"));
            t.linked_task(syndicate::name!("sender"), async move {
                let presence: AnyValue = Value::simple_record1(
                    "Present",
                    Value::from(std::process::id()).wrap()).wrap();
                let handle = syndicate::actor::next_handle();
                let assert_e = || {
                    external_event(
                        &Arc::clone(&ds.underlying.mailbox), &account, Box::new(enclose!(
                            (ds, presence, handle) move |t| t.with_entity(
                                &ds.underlying, |t, e| e.assert(t, presence, handle)))))
                };
                let retract_e = || {
                    external_event(
                        &Arc::clone(&ds.underlying.mailbox), &account, Box::new(enclose!(
                            (ds, handle) move |t| t.with_entity(
                                &ds.underlying, |t, e| e.retract(t, handle)))))
                };
                assert_e()?;
                loop {
                    account.ensure_clear_funds().await;
                    retract_e()?;
                    assert_e()?;
                }
            });
            Ok(None)
        });
        Ok(())
    }).await??;
    Ok(())
}
