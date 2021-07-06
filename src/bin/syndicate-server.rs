use futures::SinkExt;
use futures::StreamExt;

use std::future::ready;
use std::sync::Arc;

use structopt::StructOpt; // for from_args in main

use syndicate::actor::*;
use syndicate::dataspace::*;
use syndicate::error::Error;
use syndicate::error::error;
use syndicate::config;
use syndicate::relay;

use tokio::net::TcpListener;
use tokio::net::TcpStream;

use tracing::{Level, info, trace};

use tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::filter::EnvFilter::from_default_env()
        .add_directive(tracing_subscriber::filter::LevelFilter::TRACE.into());
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(true)
        .with_max_level(Level::TRACE)
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set tracing global subscriber");

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

    let ds = {
        let mut ac = Actor::new();
        let ds = ac.create(Dataspace::new());
        daemons.push(ac.start(tracing::info_span!("dataspace")));
        ds
    };

    for port in config.ports.clone() {
        let ds = Arc::clone(&ds);
        let config = Arc::clone(&config);
        let mut ac = Actor::new();
        ac.linked_task(tracing::info_span!("tcp", port), run_listener(ds, port, config));
        daemons.push(ac.start(tracing::info_span!("tcp", port)));
    }

    futures::future::join_all(daemons).await;
    Ok(())
}

//---------------------------------------------------------------------------

fn message_error<E: std::fmt::Display>(e: E) -> Error {
    error(&e.to_string(), false)
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
    ac: &mut Actor,
    stream: TcpStream,
    ds: Arc<Ref>,
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
                (relay::Input::Bytes(Box::pin(i)), relay::Output::Bytes(Box::pin(o)))
            }
        }
        0 => Err(error("closed before starting", false))?,
        _ => unreachable!()
    };
    Ok(relay::TunnelRelay::run(ac, i, o)?)
}

async fn run_listener(ds: Arc<Ref>, port: u16, config: Arc<config::ServerConfig>) -> ActorResult {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let ds = Arc::clone(&ds);
        let config = Arc::clone(&config);
        let ac = Actor::new();
        let id = ac.id();
        ac.boot(tracing::info_span!(parent: None, "connection", id),
                move |ac| Box::pin(run_connection(ac, stream, ds, addr, config)));
    }
}
