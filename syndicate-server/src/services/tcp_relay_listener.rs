use std::sync::Arc;

use syndicate::actor::*;

use tokio::net::TcpListener;

use crate::protocol::detect_protocol;

pub fn spawn(t: &mut Activation, gateway: Arc<Cap>, port: u16) {
    t.spawn(syndicate::name!("tcp", port),
            move |t| Ok(t.linked_task(syndicate::name!("listener"), run(gateway, port))))
}

pub async fn run(
    gateway: Arc<Cap>,
    port: u16,
) -> ActorResult {
    let listen_addr = format!("0.0.0.0:{}", port);
    tracing::info!("Listening on {}", listen_addr);
    let listener = TcpListener::bind(listen_addr).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let gateway = Arc::clone(&gateway);
        Actor::new().boot(syndicate::name!(parent: None, "tcp"),
                          move |t| Ok(t.linked_task(
                              tracing::Span::current(),
                              detect_protocol(t.facet.clone(), stream, gateway, addr))));
    }
}
