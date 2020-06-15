use futures::{SinkExt, StreamExt, poll};
use std::task::Poll;
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use syndicate::packets::{ClientCodec, C2S, S2C, Action};
use syndicate::value::{Value, IOValue};

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "a", default_value = "1")]
    action_count: u32,

    #[structopt(short = "b", default_value = "0")]
    bytes_padding: usize,
}

#[inline]
fn says(who: IOValue, what: IOValue) -> IOValue {
    let mut r = Value::simple_record("Says", 2);
    r.fields_vec_mut().push(who);
    r.fields_vec_mut().push(what);
    r.finish().wrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args();

    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?, ClientCodec::new());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;

    let padding: IOValue = Value::ByteString(vec![0; config.bytes_padding]).wrap();

    loop {
        let mut actions = vec![];
        for _ in 0..config.action_count {
            actions.push(Action::Message(says(Value::from("producer").wrap(),
                                              padding.clone())));
        }
        frames.send(C2S::Turn(actions)).await?;

        loop {
            match poll!(frames.next()) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    print!("Server closed connection");
                    return Ok(());
                }
                Poll::Ready(Some(res)) => {
                    let p = res?;
                    print!("{:?}\n", p);
                    if let S2C::Ping() = p { frames.send(C2S::Pong()).await? }
                }
            }
        }
    }
}
