use futures::SinkExt;
use futures::StreamExt;

use std::future::ready;
use std::io;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::dataspace::*;
use syndicate::during::DuringResult;
use syndicate::error::Error;
use syndicate::error::error;
use syndicate::relay;
use syndicate::schemas::internal_protocol::_Any;
use syndicate::schemas::gatekeeper;
use syndicate::sturdy;

use syndicate::value::NestedValue;

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UnixListener;
use tokio::net::UnixStream;

use tungstenite::Message;

#[derive(Clone, StructOpt)]
struct ServerConfig {
    #[structopt(short = "p", long = "port", default_value = "8001")]
    ports: Vec<u16>,

    #[structopt(short = "s", long = "socket")]
    sockets: Vec<PathBuf>,

    #[structopt(long)]
    debt_reporter: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;

    let config = Arc::new(ServerConfig::from_args());

    {
        const BRIGHT_GREEN: &str = "\x1b[92m";
        const RED: &str = "\x1b[31m";
        const GREEN: &str = "\x1b[32m";
        const NORMAL: &str = "\x1b[0m";
        const BRIGHT_YELLOW: &str = "\x1b[93m";

        tracing::info!(r"{}    ______   {}", GREEN, NORMAL);
        tracing::info!(r"{}   /    {}\_{}\{}  ", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{}  /  {},{}__/{}  \ {}                         ____           __", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{} /{}\__/  \{},{}  \{}   _______  ______  ____/ /_/________  / /____", GREEN, BRIGHT_GREEN, RED, GREEN, NORMAL);
        tracing::info!(r"{} \{}/  \__/   {}/{}  / ___/ / / / __ \/ __  / / ___/ __ \/ __/ _ \", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{}  \  {}'{}  \__{}/ {} _\_ \/ /_/ / / / / /_/ / / /__/ /_/ / /_/  __/", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{}   \____{}/{}_/ {} /____/\__, /_/ /_/\____/_/\___/\__/_/\__/\___/", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"                  /____/");

        // tracing::info!(r"   {}   __{}__{}__   {}", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // tracing::info!(r"   {}  /{}_/  \_{}\  {}", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // tracing::info!(r"   {} /  \__/  \ {}                         __             __", BRIGHT_GREEN, NORMAL);
        // tracing::info!(r"   {}/{}\__/  \__/{}\{}   _______  ______  ____/ /__________  / /____", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // tracing::info!(r"   {}\{}/  \__/  \{}/{}  / ___/ / / / __ \/ __  / / ___/ __ \/ __/ _ \", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // tracing::info!(r"   {} \__/  \__/ {} _\_ \/ /_/ / / / / /_/ / / /__/ /_/ / /_/  __/", BRIGHT_GREEN, NORMAL);
        // tracing::info!(r"   {}  \_{}\__/{}_/ {} /____/\__, /_/ /_/\____/_/\___/\__/_/\__/\___/", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        // tracing::info!(r"                    /____/");

        tracing::info!(r"");
        tracing::info!(r" {}version {}{}", BRIGHT_YELLOW, env!("CARGO_PKG_VERSION"), NORMAL);
        tracing::info!(r"");
        tracing::info!(r" documentation & reference material: https://syndicate-lang.org/");
        tracing::info!(r" source code & bugs: https://git.syndicate-lang.org/syndicate-lang/syndicate-rs");
        tracing::info!(r"");
    }

    let mut daemons = Vec::new();

    tracing::trace!("startup");

    if config.debt_reporter {
        syndicate::actor::start_debt_reporter();
    }

    let ds = Cap::new(&Actor::create_and_start(syndicate::name!("dataspace"), Dataspace::new()));
    let gateway = Cap::guard(&Actor::create_and_start(
        syndicate::name!("gateway"),
        syndicate::entity(Arc::clone(&ds)).on_asserted(handle_resolve)));
    {
        let ds = Arc::clone(&ds);
        Actor::new().boot(syndicate::name!("rootcap"), move |t| {
            use syndicate::schemas::gatekeeper;
            let key = vec![0; 16];
            let sr = sturdy::SturdyRef::mint(_Any::new("syndicate"), &key);
            tracing::info!(rootcap = debug(&_Any::from(&sr)));
            tracing::info!(rootcap = display(sr.to_hex()));
            ds.assert(t, &gatekeeper::Bind { oid: sr.oid.clone(), key, target: ds.clone() });
            Ok(())
        });
    }

    for port in config.ports.clone() {
        let gateway = Arc::clone(&gateway);
        daemons.push(Actor::new().boot(
            syndicate::name!("tcp", port),
            move |t| Ok(t.state.linked_task(syndicate::name!("listener"),
                                            run_tcp_listener(gateway, port)))));
    }

    for path in config.sockets.clone() {
        let gateway = Arc::clone(&gateway);
        daemons.push(Actor::new().boot(
            syndicate::name!("unix", socket = debug(path.to_str().expect("representable UnixListener path"))),
            move |t| Ok(t.state.linked_task(syndicate::name!("listener"),
                                            run_unix_listener(gateway, path)))));
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
                Ok(None), // we're about to see the end of the stream, so ignore this
        },
        Err(e) => Err(message_error(e)),
    }
}

#[doc(hidden)]
struct ExitListener;

impl Entity<()> for ExitListener {
    fn exit_hook(&mut self, _t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        tracing::info!(exit_status = debug(exit_status), "disconnect");
        Ok(())
    }
}

fn run_connection(
    ac: ActorRef,
    i: relay::Input,
    o: relay::Output,
    gateway: Arc<Cap>,
) -> ActorResult {
    Activation::for_actor(&ac, Account::new(syndicate::name!("start-session")), |t| {
        let exit_listener = t.state.create(ExitListener);
        t.state.add_exit_hook(&exit_listener);
        relay::TunnelRelay::run(t, i, o, Some(gateway), None);
        Ok(())
    })
}

async fn detect_protocol(
    ac: ActorRef,
    stream: TcpStream,
    gateway: Arc<Cap>,
    addr: std::net::SocketAddr,
) -> ActorResult {
    let (i, o) = {
        let mut buf = [0; 1]; // peek at the first byte to see what kind of connection to expect
        match stream.peek(&mut buf).await? {
            1 => match buf[0] {
                b'G' /* ASCII 'G' for "GET" */ => {
                    tracing::info!(protocol = display("websocket"), peer = debug(addr));
                    let s = tokio_tungstenite::accept_async(stream).await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    let (o, i) = s.split();
                    let i = i.filter_map(|r| ready(extract_binary_packets(r).transpose()));
                    let o = o.sink_map_err(message_error).with(|bs| ready(Ok(Message::Binary(bs))));
                    (relay::Input::Packets(Box::pin(i)), relay::Output::Packets(Box::pin(o)))
                },
                _ => {
                    tracing::info!(protocol = display("raw"), peer = debug(addr));
                    let (i, o) = stream.into_split();
                    (relay::Input::Bytes(Box::pin(i)),
                     relay::Output::Bytes(Box::pin(o /* BufWriter::new(o) */)))
                }
            }
            0 => Err(error("closed before starting", _Any::new(false)))?,
            _ => unreachable!()
        }
    };
    run_connection(ac, i, o, gateway)
}

async fn run_tcp_listener(
    gateway: Arc<Cap>,
    port: u16,
) -> ActorResult {
    let listen_addr = format!("0.0.0.0:{}", port);
    tracing::info!("Listening on {}", listen_addr);
    let listener = TcpListener::bind(listen_addr).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let gateway = Arc::clone(&gateway);
        let ac = Actor::new();
        ac.boot(syndicate::name!(parent: None, "tcp"),
                move |t| Ok(t.state.linked_task(
                    tracing::Span::current(),
                    detect_protocol(t.actor.clone(), stream, gateway, addr))));
    }
}

async fn run_unix_listener(
    gateway: Arc<Cap>,
    path: PathBuf,
) -> ActorResult {
    let path_str = path.to_str().expect("representable UnixListener path");
    tracing::info!("Listening on {:?}", path_str);
    let listener = bind_unix_listener(&path).await?;
    loop {
        let (stream, _addr) = listener.accept().await?;
        let peer = stream.peer_cred()?;
        let gateway = Arc::clone(&gateway);
        let ac = Actor::new();
        ac.boot(syndicate::name!(parent: None,
                                 "unix",
                                 pid = debug(peer.pid().unwrap_or(-1)),
                                 uid = peer.uid()),
                move |t| Ok(t.state.linked_task(
                    tracing::Span::current(),
                    {
                        let ac = t.actor.clone();
                        async move {
                            tracing::info!(protocol = display("unix"));
                            let (i, o) = stream.into_split();
                            run_connection(ac,
                                           relay::Input::Bytes(Box::pin(i)),
                                           relay::Output::Bytes(Box::pin(o)),
                                           gateway)
                        }
                    })));
    }
}

async fn bind_unix_listener(path: &PathBuf) -> Result<UnixListener, Error> {
    match UnixListener::bind(path) {
        Ok(s) => Ok(s),
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            // Potentially-stale socket file sitting around. Try
            // connecting to it to see if it is alive, and remove it
            // if not.
            match UnixStream::connect(path).await {
                Ok(_probe) => Err(e)?, // Someone's already there! Give up.
                Err(f) if f.kind() == io::ErrorKind::ConnectionRefused => {
                    // Try to steal the socket.
                    tracing::info!("Cleaning stale socket");
                    std::fs::remove_file(path)?;
                    Ok(UnixListener::bind(path)?)
                }
                Err(f) => {
                    tracing::error!(error = debug(f),
                                    "Problem while probing potentially-stale socket");
                    return Err(e)? // signal the *original* error, not the probe error
                }
            }
        },
        Err(e) => Err(e)?,
    }
}

//---------------------------------------------------------------------------

fn handle_resolve(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: gatekeeper::Resolve,
) -> DuringResult<Arc<Cap>> {
    use syndicate::schemas::dataspace;

    let gatekeeper::Resolve { sturdyref, observer } = a;
    let queried_oid = sturdyref.oid.clone();
    let handler = syndicate::entity(observer)
        .on_asserted(move |observer, t, a: _Any| {
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
                    if let Some(h) = observer.assert(t, _Any::domain(target)) {
                        Ok(Some(Box::new(move |_observer, t| Ok(t.retract(h)))))
                    } else {
                        Ok(None)
                    }
                }
            }
        })
        .create_cap(t.state);
    if let Some(oh) = ds.assert(t, &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: syndicate_macros::pattern!("<bind =queried_oid $ $>"),
        observer: handler,
    }) {
        Ok(Some(Box::new(move |_ds, t| Ok(t.retract(oh)))))
    } else {
        Ok(None)
    }
}
