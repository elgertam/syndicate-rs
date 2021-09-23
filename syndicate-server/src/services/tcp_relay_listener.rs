use preserves_schema::Codec;

use std::convert::TryFrom;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use tokio::net::TcpListener;

use crate::language::language;
use crate::protocol::detect_protocol;
use crate::schemas::internal_services;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>, gateway: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, ds, language(), <require-service $spec: internal_services::TcpRelayListener>,
                   |t| {
                       Supervisor::start(
                           t,
                           syndicate::name!(parent: None, "relay", addr = ?spec),
                           SupervisorConfiguration::default(),
                           enclose!((ds, gateway) move |t|
                                    enclose!((ds, gateway, spec) run(t, ds, gateway, spec))));
                       Ok(())
                   }))
    });
}

fn run(
    t: &'_ mut Activation,
    ds: Arc<Cap>,
    gateway: Arc<Cap>,
    spec: internal_services::TcpRelayListener,
) -> ActorResult {
    let host = spec.addr.host.clone();
    let port = u16::try_from(&spec.addr.port).map_err(|_| "Invalid TCP port number")?;
    {
        let spec = language().unparse(&spec);
        ds.assert(t, &(), &syndicate_macros::template!("<service-running =spec>"));
    }
    let parent_span = tracing::Span::current();
    t.linked_task(syndicate::name!("listener"), async move {
        let listen_addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(listen_addr).await?;
        tracing::info!("listening");
        loop {
            let (stream, addr) = listener.accept().await?;
            Actor::new().boot(syndicate::name!(parent: parent_span.clone(), "conn"),
                              enclose!((gateway) move |t| Ok(t.linked_task(
                                  tracing::Span::current(),
                                  detect_protocol(t.facet.clone(), stream, gateway, addr)))));
        }
    });
    Ok(())
}
