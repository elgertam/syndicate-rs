use futures::{SinkExt, StreamExt};

use preserves::value::PackedReader;
use preserves::value::PackedWriter;
use preserves::value::Reader;
use preserves::value::Writer;

use std::convert::TryFrom;
use std::future::Ready;
use std::future::ready;
use std::sync::Arc;

use structopt::StructOpt; // for from_args in main

use syndicate::actor::*;
use syndicate::dataspace::*;
use syndicate::error::Error;
use syndicate::error::error;
use syndicate::config;
use syndicate::packets;
use syndicate::peer::Peer;

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tracing::{Level, info, trace};

use tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::filter::EnvFilter::from_default_env()
        .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into());
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
        let ac = Actor::new();
        let ds = ac.create(Dataspace::new());
        daemons.push(ac.start(tracing::info_span!("dataspace")));
        ds
    };

    for port in config.ports.clone() {
        let ds = Arc::clone(&ds);
        let config = Arc::clone(&config);
        let ac = Actor::new();
        ac.linked_task(tracing::info_span!("listener", port), run_listener(ds, port, config));
    }

    futures::future::join_all(daemons).await;
    Ok(())
}

//---------------------------------------------------------------------------

fn message_error<E: std::fmt::Display>(e: E) -> Error {
    error(&e.to_string(), false)
}

fn encode_message(p: packets::Packet) -> Result<Message, Error> {
    let mut bs = Vec::with_capacity(128);
    PackedWriter::new(&mut bs).write(&(&p).into())?;
    Ok(Message::Binary(bs))
}

fn message_encoder(p: packets::Packet) -> Ready<Result<Message, Error>>
{
    ready(encode_message(p))
}

fn message_decoder_inner(
    r: Result<Message, tungstenite::Error>,
) -> Result<Option<packets::Packet>, Error> {
    match r {
        Ok(m) => match m {
            Message::Text(_) =>
                Err("Text websocket frames are not accepted")?,
            Message::Binary(bs) => {
                let iov = PackedReader::decode_bytes(&bs).demand_next(false)?;
                let p = packets::Packet::try_from(&iov)?;
                Ok(Some(p))
            }
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

fn message_decoder(r: Result<Message, tungstenite::Error>) -> Ready<Option<Result<packets::Packet, Error>>> {
    ready(message_decoder_inner(r).transpose())
}

async fn run_connection(
    mut stream: TcpStream,
    ds: Arc<Ref>,
    addr: std::net::SocketAddr,
    config: Arc<config::ServerConfig>,
) -> ActorResult {
    let mut buf = [0; 1]; // peek at the first byte to see what kind of connection to expect
    match stream.peek(&mut buf).await? {
        1 => match buf[0] {
            71 /* ASCII 'G' for "GET" */ => {
                info!(protocol = display("websocket"), peer = debug(addr));
                let s = tokio_tungstenite::accept_async(stream).await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let (o, i) = s.split();
                let i = i.filter_map(message_decoder);
                let o = o.sink_map_err(message_error).with(message_encoder);
                let mut p = Peer::new(i, o, ds, config);
                p.run().await?
            },
            _ => {
                info!(protocol = display("raw"), peer = debug(addr));
                let (o, i) = Framed::new(stream, packets::Codec).split();
                let mut p = Peer::new(i, o, ds, config);
                p.run().await?
            }
        }
        0 => Err(error("closed before starting", false))?,
        _ => unreachable!()
    }
    Ok(())
}

async fn run_listener(ds: Arc<Ref>, port: u16, config: Arc<config::ServerConfig>) -> ActorResult {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let mut ac = Actor::new();
        let ds = Arc::clone(&ds);
        let config = Arc::clone(&config);
        ac.linked_task(tracing::info_span!("connection", id = (ac.id())),
                       run_connection(stream, ds, addr, config));
    }
}
