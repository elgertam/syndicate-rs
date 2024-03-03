use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::preserves::rec;
use syndicate::relay;
use syndicate::sturdy;
use syndicate::value::NestedValue;

use tokio::net::TcpStream;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "a", default_value = "1")]
    action_count: u32,

    #[structopt(short = "b", default_value = "0")]
    bytes_padding: usize,

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
            let padding = AnyValue::new(&vec![0u8; config.bytes_padding][..]);
            let action_count = config.action_count;
            let account = Account::new(None, None);
            t.linked_task(Some(AnyValue::symbol("sender")), async move {
                loop {
                    account.ensure_clear_funds().await;
                    facet.activate(&account, None, |t| {
                        for _ in 0..action_count {
                            ds.message(t, &(), &rec![AnyValue::symbol("Says"),
                                                     AnyValue::new("producer"),
                                                     padding.clone()]);
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
