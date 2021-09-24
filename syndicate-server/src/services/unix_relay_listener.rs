use preserves_schema::Codec;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::relay;

use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::language::language;
use crate::protocol::run_connection;
use crate::schemas::internal_services;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>, gateway: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: internal_services::UnixRelayListener>,
                   |t: &mut Activation| {
                       t.spawn_link(syndicate::name!(parent: None, "relay", addr = ?spec),
                                    enclose!((ds, gateway) |t| run(t, ds, gateway, spec)));
                       Ok(())
                   }))
    });
}

fn run(
    t: &'_ mut Activation,
    ds: Arc<Cap>,
    gateway: Arc<Cap>,
    spec: internal_services::UnixRelayListener,
) -> ActorResult {
    let path_str = spec.addr.path.clone();
    {
        let spec = language().unparse(&spec);
        ds.assert(t, &(), &syndicate_macros::template!("<service-running =spec>"));
    }
    let parent_span = tracing::Span::current();
    t.linked_task(syndicate::name!("listener"), async move {
        let listener = bind_unix_listener(&PathBuf::from(path_str)).await?;
        tracing::info!("listening");
        loop {
            let (stream, _addr) = listener.accept().await?;
            let peer = stream.peer_cred()?;
            Actor::new().boot(
                syndicate::name!(parent: parent_span.clone(), "conn",
                                 pid = ?peer.pid().unwrap_or(-1),
                                 uid = peer.uid()),
                enclose!((gateway) |t| Ok(t.linked_task(
                    tracing::Span::current(),
                    {
                        let facet = t.facet.clone();
                        async move {
                            tracing::info!(protocol = %"unix");
                            let (i, o) = stream.into_split();
                            run_connection(facet,
                                           relay::Input::Bytes(Box::pin(i)),
                                           relay::Output::Bytes(Box::pin(o)),
                                           gateway)
                        }
                    }))));
        }
    });
    Ok(())
}

async fn bind_unix_listener(path: &PathBuf) -> Result<UnixListener, Error> {
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
                Err(error) => {
                    tracing::error!(?error, "Problem while probing potentially-stale socket");
                    return Err(e)? // signal the *original* error, not the probe error
                }
            }
        },
        Err(e) => Err(e)?,
    }
}
