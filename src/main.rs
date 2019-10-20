#![recursion_limit="512"]

mod bag;
mod dataspace;
mod packets;
mod peer;
mod skeleton;
mod spaces;

use preserves::value;
use std::sync::{Mutex, Arc};
use tokio::net::TcpListener;

pub type ConnId = u64;
pub type V = value::ArcValue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spaces = Arc::new(Mutex::new(spaces::Spaces::new()));
    let mut id = 0;

    let port = 8001;
    let mut listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("Listening on port {}", port);
    loop {
        let (stream, addr) = listener.accept().await?;
        let connid = id;
        let spaces = Arc::clone(&spaces);
        id += 1;
        tokio::spawn(async move {
            match peer::Peer::new(connid, stream).await.run(spaces).await {
                Ok(_) => (),
                Err(e) => println!("Connection {:?} died with {:?}", addr, e),
            }
        });
    }
}
