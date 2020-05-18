use syndicate::{spaces, packets, ConnId, V, Syndicate};
use syndicate::peer::{Peer, ResultC2S};
use preserves::value;

use std::sync::{Mutex, Arc};
use futures::{SinkExt, StreamExt};

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use tungstenite::Message;

use structopt::StructOpt;

#[derive(Clone, StructOpt)]
struct Cli {
    #[structopt(short = "p", long = "port", default_value = "8001")]
    ports: Vec<u16>,

    #[structopt(long)]
    recv_buffer_size: Option<usize>,
    #[structopt(long)]
    send_buffer_size: Option<usize>,
}

type UnitAsyncResult = Result<(), std::io::Error>;

fn other_eio<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
}

fn translate_sink_err(e: tungstenite::Error) -> packets::EncodeError {
    packets::EncodeError::Write(other_eio(e))
}

fn encode_message(codec: &value::Codec<V, Syndicate>, p: packets::S2C) ->
    Result<Message, packets::EncodeError>
{
    let v: V = value::to_value(p)?;
    Ok(Message::Binary(codec.encode_bytes(&v)?))
}

fn message_encoder(codec: &value::Codec<V, Syndicate>)
    -> impl Fn(packets::S2C) -> futures::future::Ready<Result<Message, packets::EncodeError>> + '_
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
                        value::decoder::Error::Syntax("Text websocket frames are not accepted"))),
                    Message::Binary(ref bs) => {
                        let v = codec.decode(&mut &bs[..])?;
                        value::from_value(&v).map_err(|e| packets::DecodeError::Parse(e, v))
                    }
                    Message::Ping(_) => continue, // pings are handled by tungstenite before we see them
                    Message::Pong(_) => continue, // unsolicited pongs are to be ignored
                    Message::Close(_) => Err(packets::DecodeError::Read(value::decoder::Error::Eof)),
                }
                Err(tungstenite::Error::Io(e)) => Err(e.into()),
                Err(e) => Err(packets::DecodeError::Read(value::decoder::Error::Io(other_eio(e)))),
            }
        }
    };
}

async fn run_connection(connid: ConnId,
                        mut stream: TcpStream,
                        spaces: Arc<Mutex<spaces::Spaces>>) ->
    UnitAsyncResult
{
    let mut buf = [0; 1]; // peek at the first byte to see what kind of connection to expect
    match stream.peek(&mut buf).await? {
        1 => match buf[0] {
            71 /* ASCII 'G' for "GET" */ => {
                let s = tokio_tungstenite::accept_async(stream).await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let (o, i) = s.split();
                let codec = packets::standard_preserves_codec();
                let i = i.map(message_decoder(&codec));
                let o = o.sink_map_err(translate_sink_err).with(message_encoder(&codec));
                let mut p = Peer::new(connid, i, o);
                p.run(spaces).await?
            },
            _ => {
                println!("First byte: {:?}", buf);
                let (o, i) = Framed::new(stream, packets::Codec::standard()).split();
                let mut p = Peer::new(connid, i, o);
                p.run(spaces).await?
            }
        }
        0 => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,
                                            "closed before starting")),
        _ => unreachable!()
    }
    println!("Connection {} ({:?}) terminated normally", connid, addr);
    Ok(())
}

async fn run_listener(spaces: Arc<Mutex<spaces::Spaces>>, port: u16, args: Cli) -> UnitAsyncResult {
    let mut listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("Listening on port {}", port);
    let mut id = port as u64 + 100000000000000;
    loop {
        let (stream, addr) = listener.accept().await?;
        let connid = id;
        let spaces = Arc::clone(&spaces);
        id += 100000;
        if let Some(n) = args.recv_buffer_size { stream.set_recv_buffer_size(n)?; }
        if let Some(n) = args.send_buffer_size { stream.set_send_buffer_size(n)?; }
        tokio::spawn(async move {
            println!("Connection {} ({:?}) accepted from port {}", connid, addr, port);
            match run_connection(connid, stream, spaces).await {
                Ok(()) => println!("Connection {} ({:?}) terminated normally", connid, addr),
                Err(e) => println!("Connection {} ({:?}) terminated: {}", connid, addr, e),
            }
        });
    }
}

async fn periodic_tasks(spaces: Arc<Mutex<spaces::Spaces>>) -> UnitAsyncResult {
    let interval = core::time::Duration::from_secs(5);
    let mut delay = tokio::time::interval(interval);
    loop {
        delay.next().await.unwrap();
        {
            let mut spaces = spaces.lock().unwrap();
            spaces.cleanup();
            println!("{}", spaces.stats_string(interval));
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::from_args();

    let spaces = Arc::new(Mutex::new(spaces::Spaces::new()));
    let mut daemons = Vec::new();

    {
        let spaces = Arc::clone(&spaces);
        tokio::spawn(async move {
            periodic_tasks(spaces).await
        });
    }

    for port in args.ports.clone() {
        let spaces = Arc::clone(&spaces);
        let args = args.clone();
        daemons.push(tokio::spawn(async move {
            match run_listener(spaces, port, args).await {
                Ok(()) => (),
                Err(e) => {
                    eprintln!("Error from listener for port {}: {}", port, e);
                    std::process::exit(2)
                }
            }
        }));
    }

    futures::future::join_all(daemons).await;
    Ok(())
}
