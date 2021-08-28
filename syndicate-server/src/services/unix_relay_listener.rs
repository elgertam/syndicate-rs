use std::convert::TryFrom;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::convert::*;
use syndicate::during::entity;
use syndicate::error::Error;
use syndicate::relay;
use syndicate::schemas::dataspace::Observe;
use syndicate::value::NestedValue;

use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::protocol::run_connection;
use crate::schemas::internal_services;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>, gateway: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        let monitor = entity(())
            .on_asserted_facet({
                let ds = Arc::clone(&ds);
                move |_, t, captures| {
                    let ds = Arc::clone(&ds);
                    let gateway = Arc::clone(&gateway);
                    t.spawn_link(syndicate::name!(parent: None, "relay", addr = debug(&captures)),
                                 |t| run(t, ds, gateway, captures));
                    Ok(())
                }
            })
            .create_cap(t);
        ds.assert(t, &Observe {
            pattern: syndicate_macros::pattern!("<require-service <$ <relay-listener <unix _>>>>"),
            observer: monitor,
        });
        Ok(())
    })
}

fn run(
    t: &'_ mut Activation,
    ds: Arc<Cap>,
    gateway: Arc<Cap>,
    captures: AnyValue,
) -> ActorResult {
    let spec = internal_services::UnixRelayListener::try_from(&from_any_value(
        &captures.value().to_sequence()?[0])?)?;
    let path_str = spec.addr.path.clone();
    {
        let spec = from_io_value(&spec)?;
        ds.assert(t, syndicate_macros::template!("<service-running =spec>"));
    }
    let parent_span = tracing::Span::current();
    t.linked_task(syndicate::name!("listener"), async move {
        let listener = bind_unix_listener(&PathBuf::from(path_str)).await?;
        tracing::info!("listening");
        loop {
            let (stream, _addr) = listener.accept().await?;
            let peer = stream.peer_cred()?;
            let gateway = Arc::clone(&gateway);
            Actor::new().boot(
                syndicate::name!(parent: parent_span.clone(), "conn",
                                 pid = debug(peer.pid().unwrap_or(-1)),
                                 uid = peer.uid()),
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
