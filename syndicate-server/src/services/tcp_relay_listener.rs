use preserves_schema::Codec;

use std::convert::TryFrom;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::preserves::rec;
use syndicate::preserves::value::NestedValue;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::trace;

use tokio::net::TcpListener;

use crate::language::language;
use crate::lifecycle;
use crate::protocol::detect_protocol;
use crate::schemas::internal_services::TcpWithoutHttp;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("tcp_relay_listener")), move |t| {
        enclose!((ds) during!(t, ds, language(), <run-service $spec: TcpWithoutHttp::<AnyValue>>, |t| {
            run_supervisor(t, ds.clone(), spec)
        }));
        Ok(())
    });
}

fn run_supervisor(t: &mut Activation, ds: Arc<Cap>, spec: TcpWithoutHttp) -> ActorResult {
    Supervisor::start(
        t,
        Some(rec![AnyValue::symbol("relay"), language().unparse(&spec)]),
        SupervisorConfiguration::default(),
        enclose!((ds, spec) lifecycle::updater(ds, spec)),
        enclose!((ds) move |t| enclose!((ds, spec) run(t, ds, spec))))
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: TcpWithoutHttp) -> ActorResult {
    lifecycle::terminate_on_service_restart(t, &ds, &spec);

    let httpd = t.named_field("httpd", None::<Arc<Cap>>);

    {
        let ad = spec.addr.clone();
        let ad2 = ad.clone();
        let gk = spec.gatekeeper.clone();
        enclose!((ds, httpd) during!(t, ds, language(),
            <run-service <relay-listener #(&language().unparse(&ad)) #(&AnyValue::domain(gk)) $h>>, |t: &mut Activation| {
                if let Some(h) = h.value().as_embedded().cloned() {
                    h.assert(t, language(), &syndicate::schemas::http::HttpListener { port: ad2.port.clone() });
                    *t.get_mut(&httpd) = Some(h.clone());
                    t.on_stop(enclose!((httpd) move |t| {
                        let f = t.get_mut(&httpd);
                        if *f == Some(h.clone()) { *f = None; }
                        Ok(())
                    }));
                }
                Ok(())
            }));
    }

    let TcpWithoutHttp { addr, gatekeeper } = spec.clone();

    let host = addr.host.clone();
    let port = u16::try_from(&addr.port).map_err(|_| "Invalid TCP port number")?;
    let facet = t.facet_ref();
    let trace_collector = t.trace_collector();
    t.linked_task(Some(AnyValue::symbol("listener")), async move {
        let listen_addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(listen_addr).await?;

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
            let (stream, addr) = listener.accept().await?;
            let gatekeeper = gatekeeper.clone();
            let name = Some(rec![AnyValue::symbol("tcp"), AnyValue::new(format!("{}", &addr))]);
            let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("connect"));
            let account = Account::new(name.clone(), trace_collector.clone());
            if !facet.activate(
                &account, cause, enclose!((trace_collector, httpd) move |t| {
                    let httpd = t.get(&httpd).clone();
                    t.spawn(name, move |t| {
                        Ok(t.linked_task(None, {
                            let facet = t.facet_ref();
                            async move {
                                detect_protocol(trace_collector,
                                                facet,
                                                stream,
                                                gatekeeper,
                                                httpd,
                                                addr,
                                                port).await?;
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
