use preserves_schema::Codec;

use std::convert::TryFrom;
use std::io::Read;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::preserves::Map;
use syndicate::preserves::signed_integer::SignedInteger;
use syndicate::schemas::http;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services::HttpRouter;
use crate::schemas::internal_services::HttpStaticFileServer;

use syndicate_macros::during;
use syndicate_macros::template;

lazy_static::lazy_static! {
    pub static ref MIME_TABLE: Map<String, String> = load_mime_table(&mime_table_path()).unwrap_or_default();
}

pub fn mime_table_path() -> String {
    match std::env::var("SYNDICATE_MIME_TYPES") {
        Ok(p) => p,
        Err(_) => "/etc/mime.types".to_string(),
    }
}

pub fn load_mime_table(path: &str) -> Result<Map<String, String>, std::io::Error> {
    let mut table = Map::new();
    let file = std::fs::read_to_string(path)?;
    for line in file.split('\n') {
        if line.starts_with('#') {
            continue;
        }
        let pieces = line.split(&[' ', '\t'][..]).collect::<Vec<&str>>();
        for i in 1..pieces.len() {
            table.insert(pieces[i].to_string(), pieces[0].to_string());
        }
    }
    Ok(table)
}

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("http_router_listener")), move |t| {
        enclose!((ds) during!(t, ds, language(), <run-service $spec: HttpRouter::<Arc<Cap>>>, |t: &mut Activation| {
            let spec_v = language().unparse(&spec);
            t.spawn_link(Some(template!("<http_router =spec_v>")), enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }));
        enclose!((ds) during!(t, ds, language(), <run-service $spec: HttpStaticFileServer>, |t: &mut Activation| {
            let spec_v = language().unparse(&spec);
            t.spawn_link(Some(template!("<http_static_file_server =spec_v>")),
                         enclose!((ds) |t| run_static_file_server(t, ds, spec)));
            Ok(())
        }));
        Ok(())
    });
}

#[derive(Debug, Clone)]
struct ActiveHandler {
    cap: Arc<Cap>,
    terminated: Arc<Field<bool>>,
}
type MethodTable = Map<http::MethodPattern, Vec<ActiveHandler>>;
type HostTable = Map<http::HostPattern, Map<http::PathPattern, MethodTable>>;
type RoutingTable = Map<SignedInteger, HostTable>;

fn request_host(value: &http::RequestHost) -> Option<String> {
    match value {
        http::RequestHost::Present(h) => Some(h.to_owned()),
        http::RequestHost::Absent => None,
    }
}

fn run(t: &mut Activation, ds: Arc<Cap>, spec: HttpRouter) -> ActorResult {
    ds.assert(t, language(), &lifecycle::started(&spec));
    ds.assert(t, language(), &lifecycle::ready(&spec));
    let httpd = spec.httpd;

    let routes: Arc<Field<RoutingTable>> = t.named_field("routes", Map::new());

    enclose!((httpd, routes) during!(t, httpd, language(), <http-bind _ $port _ _ _>, |t: &mut Activation| {
        let port1 = port.clone();
        enclose!((httpd, routes) during!(t, httpd, language(), <http-listener #(&port1)>, enclose!((routes, port) |t: &mut Activation| {
            let port2 = port.clone();
            during!(t, httpd, language(), <http-bind $host #(&port2) $method $path $handler>, |t: &mut Activation| {
                tracing::debug!("+HTTP binding {:?} {:?} {:?} {:?} {:?}", host, port, method, path, handler);
                let port = match port.to_signed_integer() { Ok(v) => v.into_owned(), Err(_) => return Ok(()) };
                let host = language().parse::<http::HostPattern>(&host)?;
                let path = language().parse::<http::PathPattern>(&path)?;
                let method = language().parse::<http::MethodPattern>(&method)?;
                let handler_cap = match handler.to_embedded() { Ok(v) => v.into_owned(), Err(_) => return Ok(()) };
                let handler_terminated = t.named_field("handler-terminated", false);
                t.get_mut(&routes)
                    .entry(port.clone()).or_default()
                    .entry(host.clone()).or_default()
                    .entry(path.clone()).or_default()
                    .entry(method.clone()).or_default()
                    .push(ActiveHandler {
                        cap: handler_cap.clone(),
                        terminated: handler_terminated,
                    });
                t.on_stop(enclose!((routes, method, path, host, port) move |t| {
                    tracing::debug!("-HTTP binding {:?} {:?} {:?} {:?} {:?}", host, port, method, path, handler);
                    let port_map = t.get_mut(&routes);
                    let host_map = port_map.entry(port.clone()).or_default();
                    let path_map = host_map.entry(host.clone()).or_default();
                    let method_map = path_map.entry(path.clone()).or_default();
                    let handler_vec = method_map.entry(method.clone()).or_default();
                    let handler = {
                        let i = handler_vec.iter().position(|a| a.cap == handler_cap)
                            .expect("Expected an index of an active handler to remove");
                        handler_vec.swap_remove(i)
                    };
                    if handler_vec.is_empty() {
                        method_map.remove(&method);
                    }
                    if method_map.is_empty() {
                        path_map.remove(&path);
                    }
                    if path_map.is_empty() {
                        host_map.remove(&host);
                    }
                    if host_map.is_empty() {
                        port_map.remove(&port);
                    }
                    *t.get_mut(&handler.terminated) = true;
                    Ok(())
                }));
                Ok(())
            });
            Ok(())
        })));
        Ok(())
    }));

    during!(t, httpd, language(), <request $req $res>, |t: &mut Activation| {
        let req = match language().parse::<http::HttpRequest>(&req) { Ok(v) => v, Err(_) => return Ok(()) };
        let res = match res.to_embedded() { Ok(v) => v, Err(_) => return Ok(()) };
        let res = res.as_ref();

        tracing::trace!("Looking up handler for {:#?} in {:#?}", &req, &t.get(&routes));

        let host_map = match t.get(&routes).get(&req.port) {
            Some(host_map) => host_map,
            None => return send_empty(t, res, 404, "Not found"),
        };

        let methods = match request_host(&req.host).and_then(|h| try_hostname(host_map, http::HostPattern::Host(h), &req.path).transpose()).transpose()? {
            Some(methods) => methods,
            None => match try_hostname(host_map, http::HostPattern::Any, &req.path)? {
                Some(methods) => methods,
                None => return send_empty(t, res, 404, "Not found"),
            }
        };

        let handlers = match methods.get(&http::MethodPattern::Specific(req.method.clone())) {
            Some(handlers) => handlers,
            None => match methods.get(&http::MethodPattern::Any) {
                Some(handlers) => handlers,
                None => {
                    let allowed = methods.keys().map(|k| match k {
                        http::MethodPattern::Specific(m) => m.to_uppercase(),
                        http::MethodPattern::Any => unreachable!(),
                    }).collect::<Vec<String>>().join(", ");
                    res.message(t, language(), &http::HttpResponse::Status {
                        code: 405.into(), message: "Method Not Allowed".into() });
                    res.message(t, language(), &http::HttpResponse::Header {
                        name: "allow".into(), value: allowed });
                    return send_done(t, res);
                }
            }
        };

        if handlers.len() > 1 {
            tracing::warn!(?req, "Too many handlers available");
        }
        let ActiveHandler { cap, terminated } = handlers.first().expect("Nonempty handler set").clone();
        tracing::trace!("Handler for {:?} is {:?}", &req, &cap);

        t.dataflow(enclose!((terminated, req, res) move |t| {
            if *t.get(&terminated) {
                tracing::trace!("Handler for {:?} terminated", &req);
                send_empty(t, &res, 500, "Internal Server Error")?;
            }
            Ok(())
        }))?;

        cap.assert(t, language(), &http::HttpContext { req, res: res.clone() });
        Ok(())
    });

    Ok(())
}

fn send_done(t: &mut Activation, res: &Arc<Cap>) -> ActorResult {
    res.message(t, language(), &http::HttpResponse::Done {
        chunk: http::Chunk::Bytes(vec![]) });
    Ok(())
}
fn send_empty(t: &mut Activation, res: &Arc<Cap>, code: u16, message: &str) -> ActorResult {
    res.message(t, language(), &http::HttpResponse::Status {
        code: code.into(), message: message.into() });
    send_done(t, res)
}

fn path_pattern_matches(path_pat: &http::PathPattern, path: &Vec<String>) -> bool {
    let mut path_iter = path.iter();
    for pat_elem in path_pat.0.iter() {
        match pat_elem {
            http::PathPatternElement::Label(v) => match path_iter.next() {
                Some(path_elem) => {
                    if v != path_elem {
                        return false;
                    }
                }
                None => return false,
            },
            http::PathPatternElement::Wildcard => match path_iter.next() {
                Some(_) => (),
                None => return false,
            },
            http::PathPatternElement::Rest => return true,
        }
    }
    match path_iter.next() {
        Some(_more) => false,
        None => true,
    }
}

fn try_hostname<'table>(
    host_map: &'table HostTable,
    host_pat: http::HostPattern,
    path: &Vec<String>,
) -> Result<Option<&'table MethodTable>, Error> {
    match host_map.get(&host_pat) {
        None => Ok(None),
        Some(path_table) => {
            for (path_pat, method_table) in path_table.iter() {
                tracing::trace!("Checking path {:?} against pattern {:?}", &path, &path_pat);
                if path_pattern_matches(path_pat, path) {
                    return Ok(Some(method_table));
                }
            }
            Ok(None)
        }
    }
}

fn render_dir(path: std::path::PathBuf) -> Result<(Vec<u8>, Option<&'static str>), Error> {
    let mut body = String::new();
    for entry in std::fs::read_dir(&path)? {
        if let Ok(entry) = entry {
            let is_dir = entry.metadata().map(|m| m.is_dir()).unwrap_or(false);
            let name = entry.file_name().to_string_lossy()
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('\'', "&apos;")
                .replace('"', "&quot;") + (if is_dir { "/" } else { "" });
            body.push_str(&format!("<a href=\"{}\">{}</a><br>\n", name, name));
        }
    }
    Ok((body.into_bytes(), Some("text/html")))
}

impl HttpStaticFileServer {
    fn respond(&mut self, t: &mut Activation, req: &http::HttpRequest, res: &Arc<Cap>) -> ActorResult {
        let path_prefix_elements = usize::try_from(&self.path_prefix_elements)
            .map_err(|_| "Bad pathPrefixElements")?;
        let mut is_index = false;

        let mut path = req.path[path_prefix_elements..].iter().cloned().collect::<Vec<String>>();
        if let Some(e) = path.last_mut() {
            if e.len() == 0 {
                *e = "index.html".into();
                is_index = true;
            }
        }

        let mut realpath = std::path::PathBuf::from(&self.dir);
        for element in path.into_iter() {
            if element.contains('/') || element.starts_with('.') { Err("Invalid path element")?; }
            realpath.push(element);
        }

        let (body, mime_type) = match std::fs::File::open(&realpath) {
            Err(_) => {
                if is_index {
                    realpath.pop();
                }
                if std::fs::metadata(&realpath).is_ok_and(|m| m.is_dir()) {
                    render_dir(realpath)?
                } else {
                    return send_empty(t, res, 404, "Not found")
                }
            },
            Ok(mut fh) => {
                if fh.metadata().is_ok_and(|m| m.is_dir()) {
                    drop(fh);
                    res.message(t, language(), &http::HttpResponse::Status {
                        code: 301.into(), message: "Moved permanently".into() });
                    res.message(t, language(), &http::HttpResponse::Header {
                        name: "location".into(), value: format!("/{}/", req.path.join("/")) });
                    return send_done(t, res);
                } else {
                    let mut buf = Vec::new();
                    fh.read_to_end(&mut buf)?;
                    if let Some(extension) = realpath.extension().and_then(|e| e.to_str()) {
                        (buf, MIME_TABLE.get(extension).map(|m| m.as_str()))
                    } else {
                        (buf, None)
                    }
                }
            }
        };

        res.message(t, language(), &http::HttpResponse::Status {
            code: 200.into(), message: "OK".into() });
        if let Some(mime_type) = mime_type {
            res.message(t, language(), &http::HttpResponse::Header {
                name: "content-type".into(), value: mime_type.to_owned() });
        }
        res.message(t, language(), &http::HttpResponse::Done {
            chunk: http::Chunk::Bytes(body) });

        Ok(())
    }
}

impl Entity<http::HttpContext<Arc<Cap>>> for HttpStaticFileServer {
    fn assert(&mut self, t: &mut Activation, assertion: http::HttpContext<Arc<Cap>>, _handle: Handle) -> ActorResult {
        let http::HttpContext { req, res } = assertion;
        if let Err(e) = self.respond(t, &req, &res) {
            tracing::error!(?req, error=?e);
            send_empty(t, &res, 500, "Internal server error")?;
        }
        Ok(())
    }
}

fn run_static_file_server(t: &mut Activation, ds: Arc<Cap>, spec: HttpStaticFileServer) -> ActorResult {
    let object = Cap::guard(&language().syndicate, t.create(spec.clone()));
    ds.assert(t, language(), &syndicate::schemas::service::ServiceObject {
        service_name: language().unparse(&spec),
        object: AnyValue::embedded(object),
    });
    Ok(())
}
