use std::convert::TryInto;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use hyper::{Request, Response, Body, StatusCode};
use hyper::body;
use hyper::header::HeaderName;
use hyper::header::HeaderValue;

use syndicate::actor::*;
use syndicate::error::Error;
use syndicate::trace;
use syndicate::value::Map;
use syndicate::value::NestedValue;

use syndicate::schemas::http;

use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::language;

static NEXT_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn empty_response(code: StatusCode) -> Response<Body> {
    let mut r = Response::new(Body::empty());
    *r.status_mut() = code;
    r
}

type ChunkItem = Result<body::Bytes, Box<dyn std::error::Error + Send + Sync>>;

struct ResponseCollector {
    tx_res: Option<(oneshot::Sender<Response<Body>>, Response<Body>)>,
    body_tx: Option<UnboundedSender<ChunkItem>>,
}

impl ResponseCollector {
    fn new(tx: oneshot::Sender<Response<Body>>) -> Self {
        let (body_tx, body_rx) = unbounded_channel();
        let body_stream: Box<dyn futures::Stream<Item = ChunkItem> + Send> =
            Box::new(UnboundedReceiverStream::new(body_rx));
        let mut res = Response::new(body_stream.into());
        *res.status_mut() = StatusCode::OK;
        ResponseCollector {
            tx_res: Some((tx, res)),
            body_tx: Some(body_tx),
        }
    }

    fn with_res<F: FnOnce(&mut Response<Body>) -> ActorResult>(&mut self, f: F) -> ActorResult {
        if let Some((_, res)) = &mut self.tx_res {
            f(res)?;
        }
        Ok(())
    }

    fn deliver_res(&mut self) {
        if let Some((tx, res)) = std::mem::replace(&mut self.tx_res, None) {
            let _ = tx.send(res);
        }
    }

    fn add_chunk(&mut self, value: http::Chunk) -> ActorResult {
        self.deliver_res();

        if let Some(body_tx) = self.body_tx.as_mut() {
            body_tx.send(Ok(match value {
                http::Chunk::Bytes(bs) => bs.into(),
                http::Chunk::String(s) => s.as_bytes().to_vec().into(),
            }))?;
        }

        Ok(())
    }

    fn finish(&mut self, t: &mut Activation) -> ActorResult {
        self.deliver_res();
        self.body_tx = None;
        t.stop();
        Ok(())
    }
}

impl Entity<http::HttpResponse> for ResponseCollector {
    fn message(&mut self, t: &mut Activation, message: http::HttpResponse) -> ActorResult {
        match message {
            http::HttpResponse::Status { code, .. } => self.with_res(|r| {
                *r.status_mut() = StatusCode::from_u16(
                    (&code).try_into().map_err(|_| "bad status code")?)?;
                Ok(())
            }),
            http::HttpResponse::Header { name, value } => self.with_res(|r| {
                r.headers_mut().insert(HeaderName::from_bytes(name.as_bytes())?,
                                       HeaderValue::from_str(value.as_str())?);
                Ok(())
            }),
            http::HttpResponse::Chunk { chunk } => {
                self.add_chunk(*chunk)
            }
            http::HttpResponse::Done { chunk } => {
                self.add_chunk(*chunk)?;
                self.finish(t)
            }
        }
    }
}

pub async fn serve(
    trace_collector: Option<trace::TraceCollector>,
    facet: FacetRef,
    httpd: Arc<Cap>,
    mut req: Request<Body>,
    port: u16,
) -> Result<Response<Body>, Error> {
    let host = match req.headers().get("host").and_then(|v| v.to_str().ok()) {
        None => http::RequestHost::Absent,
        Some(h) => http::RequestHost::Present(match h.rsplit_once(':') {
            None => h.to_string(),
            Some((h, _port)) => h.to_string(),
        })
    };

    let uri = req.uri();
    let mut path: Vec<String> = uri.path().split('/').map(|s| s.to_string()).collect();
    path.remove(0);

    let mut query: Map<String, Vec<http::QueryValue>> = Map::new();
    for piece in uri.query().unwrap_or("").split('&').into_iter() {
        match piece.split_once('=') {
            Some((k, v)) => {
                let key = k.to_string();
                let value = v.to_string();
                match query.get_mut(&key) {
                    None => { query.insert(key, vec![http::QueryValue::String(value)]); },
                    Some(vs) => { vs.push(http::QueryValue::String(value)); },
                }
            }
            None => {
                if piece.len() > 0 {
                    let key = piece.to_string();
                    if !query.contains_key(&key) {
                        query.insert(key, vec![]);
                    }
                }
            }
        }
    }

    let mut headers: Map<String, String> = Map::new();
    for h in req.headers().into_iter() {
        match h.1.to_str() {
            Ok(v) => { headers.insert(h.0.as_str().to_string().to_lowercase(), v.to_string()); },
            Err(_) => return Ok(empty_response(StatusCode::BAD_REQUEST)),
        }
    }

    let body = match body::to_bytes(req.body_mut()).await {
        Ok(b) => http::RequestBody::Present(b.to_vec()),
        Err(_) => return Ok(empty_response(StatusCode::BAD_REQUEST)),
    };

    let account = Account::new(Some(AnyValue::symbol("http")), trace_collector);

    let (tx, rx) = oneshot::channel();

    facet.activate(&account, Some(trace::TurnCause::external("http")), |t| {
        t.facet(move |t| {
            let sreq = http::HttpRequest {
                sequence_number: NEXT_SEQ.fetch_add(1, Ordering::Relaxed).into(),
                host,
                port: port.into(),
                method: req.method().to_string().to_lowercase(),
                path,
                headers: http::Headers(headers),
                query,
                body,
            };
            tracing::debug!(?sreq);
            let srep = Cap::guard(&language().syndicate, t.create(ResponseCollector::new(tx)));
            httpd.assert(t, language(), &http::HttpContext { req: sreq, res: srep });
            Ok(())
        })?;
        Ok(())
    });

    let response_result = rx.await;

    match response_result {
        Ok(response) => Ok(response),
        Err(_ /* sender dropped */) => Ok(empty_response(StatusCode::INTERNAL_SERVER_ERROR)),
    }
}
