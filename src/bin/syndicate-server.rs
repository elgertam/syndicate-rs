use syndicate::{peer, spaces};

use std::sync::{Mutex, Arc};
use tokio::net::TcpListener;

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
                Ok(_) => println!("Connection {} ({:?}) terminated", connid, addr),
                Err(e) => println!("Connection {} ({:?}) died with {:?}", connid, addr, e),
            }
        });
    }
}
