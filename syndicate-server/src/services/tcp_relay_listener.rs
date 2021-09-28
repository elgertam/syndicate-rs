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

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>, gateway: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: TcpRelayListener>, |t| {
            Supervisor::start(
                t,
                syndicate::name!(parent: None, "relay", addr = ?spec),
                SupervisorConfiguration::default(),
                enclose!((ds, spec) lifecycle::updater(ds, spec)),
                enclose!((ds, gateway) move |t|
                         enclose!((ds, gateway, spec) run(t, ds, gateway, spec))))
        }))
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, gateway: Arc<Cap>, spec: TcpRelayListener) -> ActorResult {
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
            Actor::new().boot(
                syndicate::name!(parent: parent_span.clone(), "conn"),
                enclose!((gateway) move |t| Ok(t.linked_task(tracing::Span::current(), {
                    let facet = t.facet.clone();
                    async move {
                        detect_protocol(facet, stream, gateway, addr).await?;
                        Ok(LinkedTaskTermination::KeepFacet)
                    }
                }))));
        }
    });
    Ok(())
}
