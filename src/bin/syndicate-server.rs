use syndicate::{config, spaces, packets, ConnId};
use syndicate::peer::Peer;

use std::sync::{Mutex, Arc};
use futures::{SinkExt, StreamExt};

use tracing::{Level, error, info, trace};
use tracing_futures::Instrument;

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tungstenite::Message;

use structopt::StructOpt; // for from_args in main

type UnitAsyncResult = Result<(), std::io::Error>;

fn message_error<E: std::fmt::Display>(e: E) -> packets::Error {
    packets::Error::Message(e.to_string())
}

fn encode_message(p: packets::S2C) ->
    Result<Message, packets::Error>
{
    let mut bs = Vec::with_capacity(128);
    preserves::ser::to_writer(&mut preserves::value::PackedWriter::new(&mut bs), &p)?;
    Ok(Message::Binary(bs))
}

fn message_encoder(p: packets::S2C) -> futures::future::Ready<Result<Message, packets::Error>>
{
    futures::future::ready(encode_message(p))
}

async fn message_decoder(r: Result<Message, tungstenite::Error>) -> Option<Result<packets::C2S, packets::Error>>
{
    match r {
        Ok(ref m) => match m {
            Message::Text(_) =>
                Some(Err(preserves::error::syntax_error("Text websocket frames are not accepted"))),
            Message::Binary(ref bs) =>
                match preserves::de::from_bytes(bs) {
                    Ok(p) => Some(Ok(p)),
                    Err(e) => Some(Err(e.into())),
                },
            Message::Ping(_) =>
                None, // pings are handled by tungstenite before we see them
            Message::Pong(_) =>
                None, // unsolicited pongs are to be ignored
            Message::Close(_) =>
                Some(Err(preserves::error::eof())),
        }
        Err(tungstenite::Error::Io(e)) =>
            Some(Err(e.into())),
        Err(e) =>
            Some(Err(message_error(e))),
    }
}

async fn run_connection(connid: ConnId,
                        mut stream: TcpStream,
                        spaces: Arc<Mutex<spaces::Spaces>>,
                        addr: std::net::SocketAddr,
                        config: config::ServerConfigRef) ->
    UnitAsyncResult
{
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
                let mut p = Peer::new(connid, i, o);
                p.run(spaces, &config).await?
            },
            _ => {
                info!(protocol = display("raw"), peer = debug(addr));
                let (o, i) = Framed::new(stream, packets::Codec::new()).split();
                let mut p = Peer::new(connid, i, o);
                p.run(spaces, &config).await?
            }
        }
        0 => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,
                                            "closed before starting")),
        _ => unreachable!()
    }
    Ok(())
}

static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

async fn run_listener(spaces: Arc<Mutex<spaces::Spaces>>, port: u16, config: config::ServerConfigRef) ->
    UnitAsyncResult
{
    let mut listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let spaces = Arc::clone(&spaces);
        let config = Arc::clone(&config);
        if let Some(n) = config.recv_buffer_size { stream.set_recv_buffer_size(n)?; }
        if let Some(n) = config.send_buffer_size { stream.set_send_buffer_size(n)?; }
        tokio::spawn(async move {
            match run_connection(id, stream, spaces, addr, config).await {
                Ok(()) => info!("closed"),
                Err(e) => info!(error = display(e), "closed"),
            }
        }.instrument(tracing::info_span!("connection", id)));
    }
}

async fn periodic_tasks(spaces: Arc<Mutex<spaces::Spaces>>) -> UnitAsyncResult {
    let interval = core::time::Duration::from_secs(10);
    let mut delay = tokio::time::interval(interval);
    loop {
        delay.next().await.unwrap();
        {
            let mut spaces = spaces.lock().unwrap();
            spaces.cleanup();
            spaces.dump_stats(interval);
        }
    }
}

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
        info!(r" source code & bug tracker: https://git.leastfixedpoint.com/");
        info!(r"");
    }

    let config = Arc::new(config::ServerConfig::from_args());

    let spaces = Arc::new(Mutex::new(spaces::Spaces::new()));
    let mut daemons = Vec::new();

    {
        let spaces = Arc::clone(&spaces);
        tokio::spawn(async move {
            periodic_tasks(spaces).await
        });
    }

    trace!("startup");

    for port in config.ports.clone() {
        let spaces = Arc::clone(&spaces);
        let config = Arc::clone(&config);
        daemons.push(tokio::spawn(async move {
            info!(port, "listening");
            match run_listener(spaces, port, config).await {
                Ok(()) => (),
                Err(e) => error!("{}", e),
            }
        }.instrument(tracing::info_span!("listener", port))));
    }

    futures::future::join_all(daemons).await;
    Ok(())
}
