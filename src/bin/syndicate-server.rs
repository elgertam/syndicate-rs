use futures::SinkExt;
use futures::StreamExt;

use preserves::value::Map;
use preserves::value::NestedValue;
use preserves::value::Value;

use std::convert::TryFrom;
use std::future::ready;
use std::iter::FromIterator;
use std::sync::Arc;

use structopt::StructOpt; // for from_args in main

use syndicate::actor::*;
use syndicate::dataspace::*;
use syndicate::during::DuringResult;
use syndicate::error::Error;
use syndicate::error::error;
use syndicate::config;
use syndicate::relay;
use syndicate::schemas::internal_protocol::_Any;
use syndicate::sturdy;

use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tracing::{info, trace};

use tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;
    syndicate::actor::start_debt_reporter();

    {
        const BRIGHT_GREEN: &str = "\x1b[92m";
        const RED: &str = "\x1b[31m";
        const GREEN: &str = "\x1b[32m";
        const NORMAL: &str = "\x1b[0m";
        const BRIGHT_YELLOW: &str = "\x1b[93m";

        info!(r"{}    ______   {}", GREEN, NORMAL);
        info!(r"{}   /    {}\_{}\{}  ", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        info!(r"{}  /  {},{}__/{}  \ {}                         ____           __", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        info!(r"{} /{}\__/  \{},{}  \{}   _______  ______  ____/ /_/________  / /____", GREEN, BRIGHT_GREEN, RED, GREEN, NORMAL);
        info!(r"{} \{}/  \__/   {}/{}  / ___/ / / / __ \/ __  / / ___/ __ \/ __/ _ \", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        info!(r"{}  \  {}'{}  \__{}/ {} _\_ \/ /_/ / / / / /_/ / / /__/ /_/ / /_/  __/", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        info!(r"{}   \____{}/{}_/ {} /____/\__, /_/ /_/\____/_/\___/\__/_/\__/\___/", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        info!(r"                  /____/");

        // info!(r"   {}   __{}__{}__   {}", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // info!(r"   {}  /{}_/  \_{}\  {}", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // info!(r"   {} /  \__/  \ {}                         __             __", BRIGHT_GREEN, NORMAL);
        // info!(r"   {}/{}\__/  \__/{}\{}   _______  ______  ____/ /__________  / /____", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // info!(r"   {}\{}/  \__/  \{}/{}  / ___/ / / / __ \/ __  / / ___/ __ \/ __/ _ \", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // info!(r"   {} \__/  \__/ {} _\_ \/ /_/ / / / / /_/ / / /__/ /_/ / /_/  __/", BRIGHT_GREEN, NORMAL);
        // info!(r"   {}  \_{}\__/{}_/ {} /____/\__, /_/ /_/\____/_/\___/\__/_/\__/\___/", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // info!(r"                    /____/");

        info!(r"");
        info!(r" {}version {}{}", BRIGHT_YELLOW, env!("CARGO_PKG_VERSION"), NORMAL);
        info!(r"");
        info!(r" documentation & reference material: https://syndicate-lang.org/");
        info!(r" source code & bugs: https://git.syndicate-lang.org/syndicate-lang/syndicate-rs");
        info!(r"");
    }

    let config = Arc::new(config::ServerConfig::from_args());

    let mut daemons = Vec::new();

    trace!("startup");

    let ds = Actor::create_and_start(syndicate::name!("dataspace"), Dataspace::new());
    let gateway = Actor::create_and_start(
        syndicate::name!("gateway"),
        syndicate::entity(Arc::clone(&ds)).on_asserted(handle_resolve));
    {
        let ds = Arc::clone(&ds);
        Actor::new().boot(syndicate::name!("rootcap"), |t| Box::pin(async move {
            use syndicate::schemas::gatekeeper;
            let key = vec![0; 16];
            let sr = sturdy::SturdyRef::mint(_Any::new("syndicate"), &key);
            tracing::info!(rootcap = debug(&_Any::from(&sr)));
            tracing::info!(rootcap = display(sr.to_hex()));
            t.assert(&ds, &gatekeeper::Bind { oid: sr.oid.clone(), key, target: ds.clone() });
            Ok(())
        }));
    }

    for port in config.ports.clone() {
        let gateway = Arc::clone(&gateway);
        let config = Arc::clone(&config);
        daemons.push(Actor::new().boot(
            syndicate::name!("tcp", port),
            move |t| Box::pin(ready(Ok(t.actor.linked_task(syndicate::name!("listener"),
                                                           run_listener(gateway, port, config)))))));
    }

    futures::future::join_all(daemons).await;
    Ok(())
}

//---------------------------------------------------------------------------

fn message_error<E: std::fmt::Display>(e: E) -> Error {
    error(&e.to_string(), _Any::new(false))
}

fn extract_binary_packets(
    r: Result<Message, tungstenite::Error>,
) -> Result<Option<Vec<u8>>, Error> {
    match r {
        Ok(m) => match m {
            Message::Text(_) =>
                Err("Text websocket frames are not accepted")?,
            Message::Binary(bs) =>
                Ok(Some(bs)),
            Message::Ping(_) =>
                Ok(None), // pings are handled by tungstenite before we see them
            Message::Pong(_) =>
                Ok(None), // unsolicited pongs are to be ignored
            Message::Close(_) =>
                Err("EOF")?,
        },
        Err(e) => Err(message_error(e)),
    }
}

async fn run_connection(
    t: &mut Activation<'_>,
    stream: TcpStream,
    gateway: Arc<Ref>,
    addr: std::net::SocketAddr,
    config: Arc<config::ServerConfig>,
) -> ActorResult {
    let mut buf = [0; 1]; // peek at the first byte to see what kind of connection to expect
    let (i, o) = match stream.peek(&mut buf).await? {
        1 => match buf[0] {
            71 /* ASCII 'G' for "GET" */ => {
                info!(protocol = display("websocket"), peer = debug(addr));
                let s = tokio_tungstenite::accept_async(stream).await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let (o, i) = s.split();
                let i = i.filter_map(|r| ready(extract_binary_packets(r).transpose()));
                let o = o.sink_map_err(message_error).with(|bs| ready(Ok(Message::Binary(bs))));
                (relay::Input::Packets(Box::pin(i)), relay::Output::Packets(Box::pin(o)))
            },
            _ => {
                info!(protocol = display("raw"), peer = debug(addr));
                let (i, o) = stream.into_split();
                (relay::Input::Bytes(Box::pin(i)),
                 relay::Output::Bytes(Box::pin(o /* BufWriter::new(o) */)))
            }
        }
        0 => Err(error("closed before starting", _Any::new(false)))?,
        _ => unreachable!()
    };
    struct ExitListener;
    impl Entity for ExitListener {
        fn exit_hook(&mut self, _t: &mut Activation, exit_status: &ActorResult) -> BoxFuture<ActorResult> {
            info!(exit_status = debug(exit_status), "disconnect");
            Box::pin(ready(Ok(())))
        }
    }
    let exit_listener = t.actor.create(ExitListener);
    t.actor.add_exit_hook(&exit_listener);
    relay::TunnelRelay::run(t, i, o, Some(gateway), None);
    Ok(())
}

async fn run_listener(
    gateway: Arc<Ref>,
    port: u16,
    config: Arc<config::ServerConfig>,
) -> ActorResult {
    let listen_addr = format!("0.0.0.0:{}", port);
    tracing::info!("Listening on {}", listen_addr);
    let listener = TcpListener::bind(listen_addr).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let gateway = Arc::clone(&gateway);
        let config = Arc::clone(&config);
        let ac = Actor::new();
        ac.boot(syndicate::name!(parent: None, "connection"),
                move |t| Box::pin(run_connection(t, stream, gateway, addr, config)));
    }
}

//---------------------------------------------------------------------------

fn handle_resolve(ds: &mut Arc<Ref>, t: &mut Activation, a: _Any) -> DuringResult<Arc<Ref>> {
    use syndicate::schemas::dataspace;
    use syndicate::schemas::dataspace_patterns as p;
    use syndicate::schemas::gatekeeper;
    match gatekeeper::Resolve::try_from(&a) {
        Err(_) => Ok(None),
        Ok(gatekeeper::Resolve { sturdyref, observer }) => {
            let queried_oid = sturdyref.oid.clone();
            let handler = syndicate::entity(observer)
                .on_asserted(move |observer, t, a| {
                    let bindings = a.value().to_sequence()?;
                    let key = bindings[0].value().to_bytestring()?;
                    let unattenuated_target = bindings[1].value().to_embedded()?;
                    match sturdyref.validate_and_attenuate(key, unattenuated_target) {
                        Err(e) => {
                            tracing::warn!(sturdyref = debug(&_Any::from(&sturdyref)),
                                           "sturdyref failed validation: {}", e);
                            Ok(None)
                        },
                        Ok(target) => {
                            tracing::trace!(sturdyref = debug(&_Any::from(&sturdyref)),
                                            target = debug(&target),
                                            "sturdyref resolved");
                            let h = t.assert(observer, _Any::domain(target));
                            Ok(Some(Box::new(|_observer, t| Ok(t.retract(h)))))
                        }
                    }
                })
                .create(t.actor);
            let oh = t.assert(ds, &dataspace::Observe {
                // TODO: codegen plugin to generate pattern constructors
                pattern: p::Pattern::DCompound(Box::new(p::DCompound::Rec {
                    ctor: Box::new(p::CRec {
                        label: Value::symbol("bind").wrap(),
                        arity: 3.into(),
                    }),
                    members: Map::from_iter(vec![
                        (0.into(), p::Pattern::DLit(Box::new(p::DLit {
                            value: queried_oid,
                        }))),
                        (1.into(), p::Pattern::DBind(Box::new(p::DBind {
                            name: "key".to_owned(),
                            pattern: p::Pattern::DDiscard(Box::new(p::DDiscard)),
                        }))),
                        (2.into(), p::Pattern::DBind(Box::new(p::DBind {
                            name: "target".to_owned(),
                            pattern: p::Pattern::DDiscard(Box::new(p::DDiscard)),
                        }))),
                    ].into_iter())
                })),
                observer: handler,
            });
            Ok(Some(Box::new(|_ds, t| Ok(t.retract(oh)))))
        }
    }
}
