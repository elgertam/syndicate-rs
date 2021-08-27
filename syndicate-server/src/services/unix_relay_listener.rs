use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::error::Error;
use syndicate::relay;

use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::protocol::run_connection;

pub fn spawn(t: &mut Activation, gateway: Arc<Cap>, path: PathBuf) {
    t.spawn(syndicate::name!("unix", socket = debug(path.to_str().expect("representable UnixListener path"))),
            |t| Ok(t.linked_task(syndicate::name!("listener!"), run(gateway, path))))
}

pub async fn run(
    gateway: Arc<Cap>,
    path: PathBuf,
) -> ActorResult {
    let path_str = path.to_str().expect("representable UnixListener path");
    tracing::info!("Listening on {:?}", path_str);
    let listener = bind_unix_listener(&path).await?;
    loop {
        let (stream, _addr) = listener.accept().await?;
        let peer = stream.peer_cred()?;
        let gateway = Arc::clone(&gateway);
        Actor::new().boot(
            syndicate::name!(parent: None, "unix", pid = debug(peer.pid().unwrap_or(-1)), uid = peer.uid()),
            |t| Ok(t.linked_task(
                tracing::Span::current(),
                {
                    let facet = t.facet.clone();
                    async move {
                        tracing::info!(protocol = display("unix"));
                        let (i, o) = stream.into_split();
                        run_connection(facet,
                                       relay::Input::Bytes(Box::pin(i)),
                                       relay::Output::Bytes(Box::pin(o)),
                                       gateway)
                    }
                })));
    }
}

pub async fn bind_unix_listener(path: &PathBuf) -> Result<UnixListener, Error> {
    match UnixListener::bind(path) {
        Ok(s) => Ok(s),
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            // Potentially-stale socket file sitting around. Try
            // connecting to it to see if it is alive, and remove it
            // if not.
            match UnixStream::connect(path).await {
                Ok(_probe) => Err(e)?, // Someone's already there! Give up.
                Err(f) if f.kind() == io::ErrorKind::ConnectionRefused => {
                    // Try to steal the socket.
                    tracing::info!("Cleaning stale socket");
                    std::fs::remove_file(path)?;
                    Ok(UnixListener::bind(path)?)
                }
                Err(f) => {
                    tracing::error!(error = debug(f),
                                    "Problem while probing potentially-stale socket");
                    return Err(e)? // signal the *original* error, not the probe error
                }
            }
        },
        Err(e) => Err(e)?,
    }
}
