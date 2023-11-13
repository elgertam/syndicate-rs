use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::preserves::rec;
use syndicate::preserves::value::Map;
use syndicate::preserves::value::NestedValue;
use syndicate::preserves::value::Set;
use syndicate::schemas::http;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services::HttpRouter;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("http_router_listener")), move |t| {
        Ok(during!(t, ds, language(), <run-service $spec: HttpRouter::<AnyValue>>, |t: &mut Activation| {
            t.spawn_link(Some(rec![AnyValue::symbol("http_router"), language().unparse(&spec)]),
                         enclose!((ds) |t| run(t, ds, spec)));
            Ok(())
        }))
    });
}

type MethodTable = Map<http::MethodPattern, Set<Arc<Cap>>>;
type RoutingTable = Map<http::HostPattern, Map<http::PathPattern, MethodTable>>;

fn run(t: &mut Activation, ds: Arc<Cap>, spec: HttpRouter) -> ActorResult {
    ds.assert(t, language(), &lifecycle::started(&spec));
    ds.assert(t, language(), &lifecycle::ready(&spec));
    let httpd = spec.httpd;

    during!(t, httpd, language(), <http-bind _ $port _ _ _>, |t: &mut Activation| {
        let routes: Arc<Field<RoutingTable>> = t.named_field("routes", Map::new());
        let port1 = port.clone();
        enclose!((httpd, routes) during!(t, httpd, language(), <http-listener #(&port1)>, enclose!((routes) |t: &mut Activation| {
            during!(t, httpd, language(), <http-bind $host #(&port) $method $path $handler>, |t: &mut Activation| {
                let host = language().parse::<http::HostPattern>(&host)?;
                let path = language().parse::<http::PathPattern>(&path)?;
                let method = language().parse::<http::MethodPattern>(&method)?;
                let handler = handler.value().to_embedded()?;
                t.get_mut(&routes)
                    .entry(host.clone()).or_default()
                    .entry(path.clone()).or_default()
                    .entry(method.clone()).or_default()
                    .insert(handler.clone());
                t.on_stop(enclose!((routes, handler, method, path, host) move |t| {
                    let host_map = t.get_mut(&routes);
                    let path_map = host_map.entry(host.clone()).or_default();
                    let method_map = path_map.entry(path.clone()).or_default();
                    let handler_set = method_map.entry(method.clone()).or_default();
                    handler_set.remove(&handler);
                    if handler_set.is_empty() {
                        method_map.remove(&method);
                    }
                    if method_map.is_empty() {
                        path_map.remove(&path);
                    }
                    if path_map.is_empty() {
                        host_map.remove(&host);
                    }
                    Ok(())
                }));
                Ok(())
            });
            Ok(())
        })));
        during!(t, httpd, language(), <request $req $res>, |t: &mut Activation| {
            let req = match language().parse::<http::HttpRequest>(&req) { Ok(v) => v, Err(_) => return Ok(()) };
            let res = match res.value().to_embedded() { Ok(v) => v, Err(_) => return Ok(()) };

            let methods = match try_hostname(t, &routes, http::HostPattern::Host(req.host.clone()), &req.path)? {
                Some(methods) => methods,
                None => match try_hostname(t, &routes, http::HostPattern::Any, &req.path)? {
                    Some(methods) => methods,
                    None => {
                        res.message(t, language(), &http::HttpResponse::Status {
                            code: 404.into(), message: "Not found".into() });
                        res.message(t, language(), &http::HttpResponse::Done {
                            chunk: Box::new(http::Chunk::Bytes(vec![])) });
                        return Ok(())
                    }
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
                        res.message(t, language(), &http::HttpResponse::Done {
                            chunk: Box::new(http::Chunk::Bytes(vec![])) });
                        return Ok(())
                    }
                }
            };

            if handlers.len() > 1 {
                tracing::warn!(?req, "Too many handlers available");
            }
            let handler = handlers.first().expect("Nonempty handler set").clone();
            handler.assert(t, language(), &http::HttpContext { req, res: res.clone() });

            Ok(())
        });
        Ok(())
    });

    Ok(())
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
    true
}

fn try_hostname<'turn, 'routes>(
    t: &'routes mut Activation<'turn>,
    routes: &'routes Arc<Field<RoutingTable>>,
    host_pat: http::HostPattern,
    path: &Vec<String>,
) -> Result<Option<&'routes MethodTable>, Error> {
    match t.get(routes).get(&host_pat) {
        None => Ok(None),
        Some(path_table) => {
            for (path_pat, method_table) in path_table.iter() {
                if path_pattern_matches(path_pat, path) {
                    return Ok(Some(method_table));
                }
            }
            Ok(None)
        }
    }
}
