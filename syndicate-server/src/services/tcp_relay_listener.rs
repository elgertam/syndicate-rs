use std::convert::TryFrom;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::convert::*;
use syndicate::during::entity;
use syndicate::schemas::dataspace::Observe;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::value::NestedValue;

use tokio::net::TcpListener;

use crate::protocol::detect_protocol;
use crate::schemas::internal_services;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>, gateway: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        let monitor = entity(())
            .on_asserted_facet({
                let ds = Arc::clone(&ds);
                move |_, t, captures: AnyValue| {
                    let ds = Arc::clone(&ds);
                    let gateway = Arc::clone(&gateway);
                    Supervisor::start(
                        t,
                        syndicate::name!(parent: None, "relay", addr = ?captures),
                        SupervisorConfiguration::default(),
                        move |t| run(t, Arc::clone(&ds), Arc::clone(&gateway), captures.clone()));
                    Ok(())
                }
            })
            .create_cap(t);
        ds.assert(t, &Observe {
            pattern: syndicate_macros::pattern!("<require-service <$ <relay-listener <tcp _ _>>>>"),
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
    let spec = internal_services::TcpRelayListener::try_from(&from_any_value(
        &captures.value().to_sequence()?[0])?)?;
    let host = spec.addr.host.clone();
    let port = u16::try_from(&spec.addr.port).map_err(|_| "Invalid TCP port number")?;
    {
        let spec = from_io_value(&spec)?;
        ds.assert(t, syndicate_macros::template!("<service-running =spec>"));
    }
    let parent_span = tracing::Span::current();
    t.linked_task(syndicate::name!("listener"), async move {
        let listen_addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(listen_addr).await?;
        tracing::info!("listening");
        loop {
            let (stream, addr) = listener.accept().await?;
            let gateway = Arc::clone(&gateway);
            Actor::new().boot(syndicate::name!(parent: parent_span.clone(), "conn"),
                              move |t| Ok(t.linked_task(
                                  tracing::Span::current(),
                                  detect_protocol(t.facet.clone(), stream, gateway, addr))));
        }
    });
    Ok(())
}
