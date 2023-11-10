use preserves_schema::Codec;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::preserves::rec;
use syndicate::preserves::value::NestedValue;
use syndicate::relay;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::trace;

use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::language::language;
use crate::lifecycle;
use crate::protocol::run_connection;
use crate::schemas::internal_services::UnixRelayListener;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("unix_relay_listener")), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: UnixRelayListener::<AnyValue>>, |t| {
            Supervisor::start(
                t,
                Some(rec![AnyValue::symbol("relay"), language().unparse(&spec)]),
                SupervisorConfiguration::default(),
                enclose!((ds, spec) lifecycle::updater(ds, spec)),
                enclose!((ds) move |t| enclose!((ds, spec) run(t, ds, spec))))
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: UnixRelayListener) -> ActorResult {
    lifecycle::terminate_on_service_restart(t, &ds, &spec);
    let path_str = spec.addr.path.clone();
    let facet = t.facet.clone();
    let trace_collector = t.trace_collector();
    t.linked_task(Some(AnyValue::symbol("listener")), async move {
        let listener = bind_unix_listener(&PathBuf::from(path_str)).await?;

        {
            let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("readiness"));
            let account = Account::new(Some(AnyValue::symbol("readiness")), trace_collector.clone());
            if !facet.activate(
                &account, cause, |t| {
                    tracing::info!("listening");
                    ds.assert(t, language(), &lifecycle::ready(&spec));
                    Ok(())
                })
            {
                return Ok(LinkedTaskTermination::Normal);
            }
        }

        loop {
            let (stream, _addr) = listener.accept().await?;
            let peer = stream.peer_cred()?;
            let gatekeeper = spec.gatekeeper.clone();
            let name = Some(rec![AnyValue::symbol("unix"),
                                 AnyValue::new(peer.pid().unwrap_or(-1)),
                                 AnyValue::new(peer.uid())]);
            let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("connect"));
            let account = Account::new(name.clone(), trace_collector.clone());
            if !facet.activate(
                &account, cause, enclose!((trace_collector) move |t| {
                    t.spawn(name, |t| {
                        Ok(t.linked_task(None, {
                            let facet = t.facet.clone();
                            async move {
                                tracing::info!(protocol = %"unix");
                                let (i, o) = stream.into_split();
                                run_connection(trace_collector,
                                               facet,
                                               relay::Input::Bytes(Box::pin(i)),
                                               relay::Output::Bytes(Box::pin(o)),
                                               gatekeeper);
                                Ok(LinkedTaskTermination::KeepFacet)
                            }
                        }))
                    });
                    Ok(())
                }))
            {
                return Ok(LinkedTaskTermination::Normal);
            }
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
