use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
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
    Actor::new().boot(syndicate::name!("state-producer"), |t| {
        let ac = t.actor.clone();
        let boot_account = Arc::clone(t.account());
        Ok(t.state.linked_task(tracing::Span::current(), async move {
            let config = Config::from_args();
            let sturdyref = sturdy::SturdyRef::from_hex(&config.dataspace)?;
            let (i, o) = TcpStream::connect("127.0.0.1:8001").await?.into_split();
            Activation::for_actor(&ac, boot_account, |t| {
                relay::connect_stream(t, i, o, sturdyref, (), move |_state, t, ds| {
                    let account = Account::new(syndicate::name!("account"));
                    t.state.linked_task(syndicate::name!("sender"), async move {
                        let presence: AnyValue = Value::simple_record1(
                            "Present",
                            Value::from(std::process::id()).wrap()).wrap();
                        let handle = syndicate::actor::next_handle();
                        let assert_e = || {
                            let ds = Arc::clone(&ds);
                            let presence = presence.clone();
                            let handle = handle.clone();
                            external_event(&Arc::clone(&ds.underlying.mailbox), &account, Box::new(
                                move |t| ds.underlying.with_entity(|e| e.assert(t, presence, handle))))
                        };
                        let retract_e = || {
                            let ds = Arc::clone(&ds);
                            let handle = handle.clone();
                            external_event(&Arc::clone(&ds.underlying.mailbox), &account, Box::new(
                                move |t| ds.underlying.with_entity(|e| e.retract(t, handle))))
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
            })
        }))
    }).await??;
    Ok(())
}
