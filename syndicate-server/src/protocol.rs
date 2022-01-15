use futures::SinkExt;
use futures::StreamExt;

use std::future::ready;
use std::io;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::error::Error;
use syndicate::error::error;
use syndicate::relay;
use syndicate::value::NestedValue;

use tokio::net::TcpStream;

use tungstenite::Message;

struct ExitListener;

impl Entity<()> for ExitListener {
    fn exit_hook(&mut self, _t: &mut Activation, exit_status: &Arc<ActorResult>) -> ActorResult {
        tracing::info!(?exit_status, "disconnect");
        Ok(())
    }
}

pub fn run_io_relay(
    t: &mut Activation,
    i: relay::Input,
    o: relay::Output,
    initial_ref: Arc<Cap>,
) -> ActorResult {
    let exit_listener = t.create(ExitListener);
    t.state.add_exit_hook(&exit_listener);
    relay::TunnelRelay::run(t, i, o, Some(initial_ref), None, false);
    Ok(())
}

pub fn run_connection(
    facet: FacetRef,
    i: relay::Input,
    o: relay::Output,
    initial_ref: Arc<Cap>,
) {
    facet.activate(Account::new(syndicate::name!("start-session")),
                   |t| run_io_relay(t, i, o, initial_ref));
}

pub async fn detect_protocol(
    facet: FacetRef,
    stream: TcpStream,
    gateway: Arc<Cap>,
    addr: std::net::SocketAddr,
) -> ActorResult {
    let (i, o) = {
        let mut buf = [0; 1]; // peek at the first byte to see what kind of connection to expect
        match stream.peek(&mut buf).await? {
            1 => match buf[0] {
                b'G' /* ASCII 'G' for "GET" */ => {
                    tracing::info!(protocol = %"websocket", peer = ?addr);
                    let s = tokio_tungstenite::accept_async(stream).await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    let (o, i) = s.split();
                    let i = i.filter_map(|r| ready(extract_binary_packets(r).transpose()));
                    let o = o.sink_map_err(message_error).with(|bs| ready(Ok(Message::Binary(bs))));
                    (relay::Input::Packets(Box::pin(i)), relay::Output::Packets(Box::pin(o)))
                },
                _ => {
                    tracing::info!(protocol = %"raw", peer = ?addr);
                    let (i, o) = stream.into_split();
                    (relay::Input::Bytes(Box::pin(i)),
                     relay::Output::Bytes(Box::pin(o /* BufWriter::new(o) */)))
                }
            }
            0 => Err(error("closed before starting", AnyValue::new(false)))?,
            _ => unreachable!()
        }
    };
    run_connection(facet, i, o, gateway);
    Ok(())
}

fn message_error<E: std::fmt::Display>(e: E) -> Error {
    error(&e.to_string(), AnyValue::new(false))
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
