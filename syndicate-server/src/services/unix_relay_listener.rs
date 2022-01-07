use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::relay;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::language::language;
use crate::lifecycle;
use crate::protocol::run_connection;
use crate::schemas::internal_services::UnixRelayListener;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("unix_relay_listener"), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: UnixRelayListener>, |t| {
            Supervisor::start(
                t,
                syndicate::name!(parent: None, "relay", addr = ?spec),
                SupervisorConfiguration::default(),
                enclose!((ds, spec) lifecycle::updater(ds, spec)),
                enclose!((ds) move |t| enclose!((ds, spec) run(t, ds, spec))))
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: UnixRelayListener) -> ActorResult {
    lifecycle::terminate_on_service_restart(t, &ds, &spec);
    let path_str = spec.addr.path.clone();
    let parent_span = tracing::Span::current();
    let facet = t.facet.clone();
    t.linked_task(syndicate::name!("listener"), async move {
        let listener = bind_unix_listener(&PathBuf::from(path_str)).await?;
        facet.activate(Account::new(syndicate::name!("readiness")), |t| {
            tracing::info!("listening");
            ds.assert(t, language(), &lifecycle::ready(&spec));
            Ok(())
        })?;
        loop {
            let (stream, _addr) = listener.accept().await?;
            let peer = stream.peer_cred()?;
            let gatekeeper = spec.gatekeeper.clone();
            Actor::new().boot(
                syndicate::name!(parent: parent_span.clone(), "conn",
                                 pid = ?peer.pid().unwrap_or(-1),
                                 uid = peer.uid()),
                |t| Ok(t.linked_task(tracing::Span::current(), {
                    let facet = t.facet.clone();
                    async move {
                        tracing::info!(protocol = %"unix");
                        let (i, o) = stream.into_split();
                        run_connection(facet,
                                       relay::Input::Bytes(Box::pin(i)),
                                       relay::Output::Bytes(Box::pin(o)),
                                       gatekeeper)?;
                        Ok(LinkedTaskTermination::KeepFacet)
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
                    tracing::debug!("Cleaning stale socket");
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
