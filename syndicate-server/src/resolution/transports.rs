use preserves_schema::Codec;
use syndicate::relay;
use syndicate::schemas::trace;

use std::convert::TryFrom;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::rpc;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use syndicate::schemas::transport_address::Tcp;
use syndicate::schemas::rpc as R;
use syndicate::schemas::gatekeeper as G;

use tokio::net::TcpStream;

use crate::language;

use syndicate::enclose;
use syndicate_macros::during;
use syndicate_macros::template;

struct TransportControl;

impl Entity<G::TransportControl> for TransportControl {
    fn message(&mut self, t: &mut Activation, c: G::TransportControl) -> ActorResult {
        let G::TransportControl(G::ForceDisconnect) = c;
        t.stop_root();
        Ok(())
    }
}

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("transport_connector")), move |t| {
        during!(t, ds, language(), <q <connect-transport $addr: Tcp>>, |t| {
            let addr_v = language().unparse(&addr);
            Supervisor::start(
                t,
                Some(template!("<relay =addr_v>")),
                SupervisorConfiguration::default(),
                |_t, _s| Ok(()),
                enclose!((ds) move |t| enclose!((ds, addr) run(t, ds, addr))))
        });
        Ok(())
    });
}

fn run(t: &mut Activation, ds: Arc<Cap>, addr: Tcp) -> ActorResult {
    tracing::info!(?addr, "Connecting");
    let name = AnyValue::new(vec![AnyValue::symbol("connector"), language().unparse(&addr)]);
    let trace_collector = t.trace_collector();
    let facet = t.facet_ref();
    t.linked_task(Some(name.clone()), async move {
        let port = u16::try_from(&addr.port).map_err(|_| "Invalid TCP port number")?;
        let account = Account::new(Some(name), trace_collector.clone());
        let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("connect"));
        match TcpStream::connect((addr.host.clone(), port)).await {
            Ok(stream) => {
                let (i, o) = stream.into_split();
                let i = relay::Input::Bytes(Box::pin(i));
                let o = relay::Output::Bytes(Box::pin(o));
                let initial_oid = Some(syndicate::sturdy::Oid(0.into()));
                facet.activate(&account, cause, |t| {
                    let peer = relay::TunnelRelay::run(t, i, o, None, initial_oid, false)
                        .expect("missing initial cap on connection");
                    let control = Cap::guard(&language().syndicate, t.create(TransportControl));
                    ds.assert(t, language(), &rpc::answer(
                        language(),
                        G::ConnectTransport { addr: language().unparse(&addr) },
                        R::Result::Ok { value: language().unparse(&G::ConnectedTransport {
                            addr: language().unparse(&addr),
                            control,
                            responder_session: peer,
                        }) }));
                    Ok(())
                });
                Ok(LinkedTaskTermination::KeepFacet)
            }
            Err(e) => {
                facet.activate(&account, cause, |t| {
                    ds.assert(t, language(), &rpc::answer(
                        language(),
                        G::ConnectTransport { addr: language().unparse(&addr) },
                        R::Result::Error { error: AnyValue::symbol(format!("{:?}", e.kind())) }));
                    Ok(())
                });
                Ok(LinkedTaskTermination::Normal)
            }
        }
    });
    Ok(())
}
