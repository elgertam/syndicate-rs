use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::relay;
use syndicate::sturdy;

use tokio::net::TcpStream;

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
        relay::connect_stream(t, i, o, false, sturdyref, (), move |_state, t, ds| {
            let facet = t.facet_ref();
            let account = Account::new(None, None);
            t.linked_task(Some(AnyValue::symbol("sender")), async move {
                let presence = AnyValue::record(AnyValue::symbol("Present"), vec![
                    AnyValue::new(std::process::id())]);
                loop {
                    let mut handle = None;
                    facet.activate(&account, None, |t| {
                        handle = ds.assert(t, &(), &presence);
                        Ok(())
                    });
                    account.ensure_clear_funds().await;
                    facet.activate(&account, None, |t| {
                        if let Some(h) = handle {
                            t.retract(h);
                        }
                        Ok(())
                    });
                }
            });
            Ok(None)
        })
    }).await??;
    Ok(())
}
