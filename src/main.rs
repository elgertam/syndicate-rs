mod bag;
mod skeleton;

use std::net::{TcpListener, TcpStream};
use std::io::Result;
use preserves::value;

// use self::skeleton::Index;

mod packets {
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Error(pub String);
}

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    println!("Got {:?}", &stream);
    let codec = value::Codec::without_placeholders();
    loop {
        match codec.decode(&stream) {
            Ok(v) => codec.encoder(&mut stream).write(&v)?,
            Err(value::codec::Error::Eof) => break,
            Err(value::codec::Error::Io(e)) => return Err(e),
            Err(value::codec::Error::Syntax(s)) => {
                let v = value::to_value(packets::Error(s.to_string())).unwrap();
                codec.encoder(&mut stream).write(&v)?;
                break
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    // let i = Index::new();
    let listener = TcpListener::bind("0.0.0.0:5889")?;
    for stream in listener.incoming() {
        handle_connection(stream?);
    }
    Ok(())
}
