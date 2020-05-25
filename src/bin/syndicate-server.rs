use syndicate::{config, spaces, packets, ConnId, V, Syndicate};
use syndicate::peer::{Peer, ResultC2S};
use preserves::value;

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

fn other_eio<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
}

fn encode_message(codec: &value::Codec<V, Syndicate>, p: packets::S2C) ->
    Result<Message, std::io::Error>
{
    use serde::ser::Serialize;
    use preserves::ser::Serializer;
    let mut bs = Vec::with_capacity(128);
    let mut ser: Serializer<_, V, Syndicate> =
        Serializer::new(&mut bs, codec.encode_placeholders.as_ref());
    p.serialize(&mut ser)?;
    Ok(Message::Binary(bs))
}

fn message_encoder(codec: &value::Codec<V, Syndicate>)
    -> impl Fn(packets::S2C) -> futures::future::Ready<Result<Message, std::io::Error>> + '_
{
    return move |p| futures::future::ready(encode_message(codec, p));
}

fn message_decoder(codec: &value::Codec<V, Syndicate>)
    -> impl Fn(Result<Message, tungstenite::Error>) -> ResultC2S + '_
{
    return move |r| {
        loop {
            return match r {
                Ok(ref m) => match m {
                    Message::Text(_) => Err(packets::DecodeError::Read(
                        value::reader::err("Text websocket frames are not accepted"))),
                    Message::Binary(ref bs) => {
                        let mut buf = &bs[..];
                        let mut vs = codec.decode_all(&mut buf)?;
                        if vs.len() > 1 {
                            Err(packets::DecodeError::Read(
                                std::io::Error::new(std::io::ErrorKind::Other,
                                                    "Multiple packets in a single message")))
                        } else if vs.len() == 0 {
                            Err(packets::DecodeError::Read(
                                std::io::Error::new(std::io::ErrorKind::Other,
                                                    "Empty message")))
                        } else {
                            value::from_value(&vs[0])
                                .map_err(|e| packets::DecodeError::Parse(e, vs.swap_remove(0)))
                        }
                    }
                    Message::Ping(_) => continue, // pings are handled by tungstenite before we see them
                    Message::Pong(_) => continue, // unsolicited pongs are to be ignored
                    Message::Close(_) => Err(packets::DecodeError::Read(value::reader::eof())),
                }
                Err(tungstenite::Error::Io(e)) => Err(e.into()),
                Err(e) => Err(packets::DecodeError::Read(other_eio(e))),
            }
        }
    };
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
                let codec = packets::standard_preserves_codec();
                let i = i.map(message_decoder(&codec));
                let o = o.sink_map_err(other_eio).with(message_encoder(&codec));
                let mut p = Peer::new(connid, i, o);
                p.run(spaces, &config).await?
            },
            _ => {
                info!(protocol = display("raw"), peer = debug(addr));
                let (o, i) = Framed::new(stream, packets::Codec::standard()).split();
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
                Err(e) => {
                    error!("{}", e);
                    std::process::exit(2)
                }
            }
        }.instrument(tracing::info_span!("listener", port))));
    }

    futures::future::join_all(daemons).await;
    Ok(())
}
