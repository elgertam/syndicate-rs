use syndicate::value::Value;
use syndicate::packets::{ClientCodec, C2S, S2C, Action};
use tokio::io::AsyncRead;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::SinkExt;
use futures::StreamExt;
use std::future::Future;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut frames = Framed::new(TcpStream::connect("127.0.0.1:8001").await?,
                                 ClientCodec::standard());
    frames.send(C2S::Connect(Value::from("chat").wrap())).await?;

    // let m = Action::Message(
    //     Value::simple_record("Says", vec![
    //         Value::from("producer").wrap(),
    //         Value::from(123).wrap(),
    //     ]).wrap());
    // loop {
    //     frames.send(C2S::Turn(vec![
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //         m.clone(),
    //     ])).await?;
    // }

    let mut counter: u64 = 0;
    loop {
        counter = counter + 1;
        frames.send(C2S::Turn(vec![Action::Message(
            Value::simple_record("Says", vec![
                Value::from("producer").wrap(),
                Value::from(counter).wrap(),
            ]).wrap())])).await?;
        // match frames.poll_read() {
        //     None => (),
        //     Some(res) => match res {
        //         S2C::Ping() => frames.send(C2S::Pong()).await?,
        //         other => print!("{:?}\n", other),
        //     }
        // }
    }
}
