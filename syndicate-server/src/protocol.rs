use futures::SinkExt;
use futures::StreamExt;
use hyper::header::HeaderValue;
use hyper::service::service_fn;

use preserves_schema::Parse;
use preserves_schema::Unparse;

use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::error::error;
use syndicate::relay;
use syndicate::trace;

use tokio::net::TcpStream;

use hyper_tungstenite::tungstenite::Message;

struct ExitListener;

impl Entity<()> for ExitListener {
    fn exit_hook(&mut self, _t: &mut Activation, exit_status: &Arc<ExitStatus>) {
        tracing::info!(?exit_status, "disconnect");
    }
}

pub fn run_io_relay(
    t: &mut Activation,
    i: relay::Input,
    o: relay::Output,
    initial_ref: Arc<Cap>,
    options: relay::TunnelRelayOptions,
) -> ActorResult {
    let exit_listener = t.create(ExitListener);
    t.add_exit_hook(&exit_listener);
    relay::TunnelRelay::run(t, i, o, Some(initial_ref), None, options);
    Ok(())
}

pub fn run_connection(
    trace_collector: Option<trace::TraceCollector>,
    facet: FacetRef,
    i: relay::Input,
    o: relay::Output,
    initial_ref: Arc<Cap>,
    options: relay::TunnelRelayOptions,
) {
    let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("start-session"));
    let account = Account::new(Some(AnyValue::symbol("start-session")), trace_collector);
    facet.activate(&account, cause, |t| run_io_relay(t, i, o, initial_ref, options));
}

#[derive(Debug)]
pub struct CreditFlowControl {
    enabled: bool,
    pending_count: Arc<AtomicUsize>,
}

impl Default for CreditFlowControl {
    fn default() -> Self {
        CreditFlowControl {
            enabled: false,
            pending_count: Arc::new(0.into()),
        }
    }
}

impl relay::ExtensionHandler for CreditFlowControl {
    fn start(
        &mut self,
        t: &mut Activation,
        relay: &mut relay::TunnelRelay,
    ) -> ActorResult {
        Ok(())
    }

    fn handle_received_extension(
        &mut self,
        t: &mut Activation,
        relay: &mut relay::TunnelRelay,
        extension: syndicate::schemas::protocol::Extension<Arc<Cap>>,
    ) -> ActorResult {
        let extension = extension.unparse();
        if let Ok(_) = syndicate::schemas::protocol::EnableCreditFlowControl::parse(&extension) {
            self.enabled = true;
            tracing::info!("Enabled credit-based flow control");
            let reply = syndicate::schemas::protocol::EnabledCreditFlowControl;
            relay.send_extension(t, syndicate::schemas::protocol::Extension::parse(&reply.unparse())?)?;
        } else {
            tracing::info!(?extension, "received extension from peer");
        }
        Ok(())
    }

    fn handle_received_turn(
        &mut self,
        t: &mut Activation,
        relay: &mut relay::TunnelRelay,
        _events: &mut Vec<syndicate::schemas::protocol::TurnEvent<Arc<Cap>>>,
    ) -> ActorResult {
        if self.enabled {
            if self.pending_count.fetch_add(1, Ordering::SeqCst) == 0 {
                let pc = Arc::clone(&self.pending_count);
                t.after(std::time::Duration::from_millis(200), relay.action(move |t, tr| {
                    let count = pc.swap(0, Ordering::SeqCst);
                    let c = syndicate::schemas::protocol::IssueCredit { count: count.into() };
                    tr.send_extension(t, syndicate::schemas::protocol::Extension::parse(&c.unparse())?)
                }));
            }
        }
        Ok(())
    }
}

pub async fn detect_protocol(
    trace_collector: Option<trace::TraceCollector>,
    facet: FacetRef,
    stream: TcpStream,
    gateway: Arc<Cap>,
    httpd: Option<Arc<Cap>>,
    addr: std::net::SocketAddr,
    server_port: u16,
) -> ActorResult {
    let mut buf = [0; 1]; // peek at the first byte to see what kind of connection to expect
    match stream.peek(&mut buf).await? {
        1 => match buf[0] {
            v if v == b'[' /* Turn */ || v == b'<' /* Error and Extension */ || v >= 128 => {
                tracing::info!(protocol = %(if v >= 128 { "application/syndicate" } else { "text/syndicate" }), peer = ?addr);
                let (i, o) = stream.into_split();
                let i = relay::Input::Bytes(Box::pin(i));
                let o = relay::Output::Bytes(Box::pin(o /* BufWriter::new(o) */));
                run_connection(trace_collector, facet, i, o, gateway, Default::default());
                Ok(())
            }
            _ => {
                let upgraded = Arc::new(AtomicBool::new(false));
                let keepalive = facet.actor.keep_alive();
                let mut http = hyper::server::conn::Http::new();
                http.http1_keep_alive(true);
                http.http1_only(true);
                let service = service_fn(|mut req| enclose!(
                    (upgraded, keepalive, trace_collector, facet, gateway, httpd) async move {
                        if hyper_tungstenite::is_upgrade_request(&req) {
                            tracing::info!(protocol = %"websocket",
                                           method=%req.method(),
                                           uri=?req.uri(),
                                           host=?req.headers().get("host").unwrap_or(&HeaderValue::from_static("")));
                            let (response, websocket) = hyper_tungstenite::upgrade(&mut req, None)
                                .map_err(|e| message_error(e))?;
                            upgraded.store(true, Ordering::SeqCst);
                            tokio::spawn(enclose!(() async move {
                                let (o, i) = websocket.await?.split();
                                let i = i.filter_map(|r| ready(extract_binary_packets(r).transpose()));
                                let o = o.sink_map_err(message_error).with(|bs| ready(Ok(Message::Binary(bs))));
                                let i = relay::Input::Packets(Box::pin(i));
                                let o = relay::Output::Packets(Box::pin(o));
                                let mut options = relay::TunnelRelayOptions::default();
                                options.extension_handler = Some(Box::new(CreditFlowControl::default()));
                                run_connection(trace_collector, facet, i, o, gateway, options);
                                drop(keepalive);
                                Ok(()) as ActorResult
                            }));
                            Ok(response)
                        } else {
                            match httpd {
                                None => Ok(crate::http::empty_response(
                                    hyper::StatusCode::SERVICE_UNAVAILABLE)),
                                Some(httpd) => {
                                    tracing::info!(protocol = %"http",
                                                   method=%req.method(),
                                                   uri=?req.uri(),
                                                   host=?req.headers().get("host").unwrap_or(&HeaderValue::from_static("")));
                                    crate::http::serve(trace_collector, facet, httpd, req, server_port).await
                                }
                            }
                        }
                    }));
                http.serve_connection(stream, service).with_upgrades().await?;
                if upgraded.load(Ordering::SeqCst) {
                    tracing::debug!("serve_connection completed after upgrade to websocket");
                } else {
                    tracing::debug!("serve_connection completed after regular HTTP session");
                    facet.activate(&Account::new(None, None), None, |t| Ok(t.stop()));
                }
                Ok(())
            },
        }
        0 => Err(error("closed before starting", AnyValue::new(false)))?,
        _ => unreachable!()
    }
}

fn message_error<E: std::fmt::Display>(e: E) -> Error {
    error(&e.to_string(), AnyValue::new(false))
}

fn extract_binary_packets(
    r: Result<Message, hyper_tungstenite::tungstenite::Error>,
) -> Result<Option<Vec<u8>>, Error> {
    match r {
        Ok(m) => match m {
            Message::Text(_) =>
                Err("Text websocket frames are not accepted")?,
            Message::Binary(bs) =>
                Ok(Some(bs)),
            Message::Ping(_) =>
                Ok(None), // pings are handled by tungstenite before we see them
            Message::Pong(_) =>
                Ok(None), // unsolicited pongs are to be ignored
            Message::Close(_) =>
                Ok(None), // we're about to see the end of the stream, so ignore this
            Message::Frame(_) =>
                Err("Raw frames are not accepted")?,
        },
        Err(e) => Err(message_error(e)),
    }
}
