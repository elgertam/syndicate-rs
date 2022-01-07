use std::convert::TryFrom;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use tokio::net::TcpListener;

use crate::language::language;
use crate::lifecycle;
use crate::protocol::detect_protocol;
use crate::schemas::internal_services::TcpRelayListener;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("tcp_relay_listener"), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: TcpRelayListener>, |t| {
            Supervisor::start(
                t,
                syndicate::name!(parent: None, "relay", addr = ?spec),
                SupervisorConfiguration::default(),
                enclose!((ds, spec) lifecycle::updater(ds, spec)),
                enclose!((ds) move |t| enclose!((ds, spec) run(t, ds, spec))))
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: TcpRelayListener) -> ActorResult {
    lifecycle::terminate_on_service_restart(t, &ds, &spec);
    let host = spec.addr.host.clone();
    let port = u16::try_from(&spec.addr.port).map_err(|_| "Invalid TCP port number")?;
    let parent_span = tracing::Span::current();
    let facet = t.facet.clone();
    t.linked_task(syndicate::name!("listener"), async move {
        let listen_addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(listen_addr).await?;
        facet.activate(Account::new(syndicate::name!("readiness")), |t| {
            tracing::info!("listening");
            ds.assert(t, language(), &lifecycle::ready(&spec));
            Ok(())
        })?;
        loop {
            let (stream, addr) = listener.accept().await?;
            let gatekeeper = spec.gatekeeper.clone();
            Actor::new().boot(
                syndicate::name!(parent: parent_span.clone(), "conn"),
                move |t| Ok(t.linked_task(tracing::Span::current(), {
                    let facet = t.facet.clone();
                    async move {
                        detect_protocol(facet, stream, gatekeeper, addr).await?;
                        Ok(LinkedTaskTermination::KeepFacet)
                    }
                })));
        }
    });
    Ok(())
}
