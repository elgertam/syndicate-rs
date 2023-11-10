use notify::DebouncedEvent;
use notify::Watcher;
use notify::RecursiveMode;
use notify::watcher;

use syndicate::preserves::rec;

use std::fs;
use std::future;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use syndicate::actor::*;
use syndicate::error::Error;
use syndicate::enclose;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::trace;
use syndicate::value::BinarySource;
use syndicate::value::BytesBinarySource;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::NoEmbeddedDomainCodec;
use syndicate::value::Reader;
use syndicate::value::ViaCodec;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services;
use crate::script;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>) {
    t.spawn(Some(AnyValue::symbol("config_watcher")), move |t| {
        Ok(during!(t, config_ds, language(), <run-service $spec: internal_services::ConfigWatcher::<AnyValue>>, |t| {
            Supervisor::start(
                t,
                Some(rec![AnyValue::symbol("config"), AnyValue::new(spec.path.clone())]),
                SupervisorConfiguration::default(),
                enclose!((config_ds, spec) lifecycle::updater(config_ds, spec)),
                enclose!((config_ds) move |t| enclose!((config_ds, spec) run(t, config_ds, spec))))
        }))
    });
}

fn convert_notify_error(e: notify::Error) -> Error {
    syndicate::error::error(&format!("Notify error: {:?}", e), AnyValue::new(false))
}

fn process_existing_file(
    t: &mut Activation,
    mut env: script::Env,
) -> io::Result<Option<FacetId>> {
    let mut contents = fs::read(&env.path)?;
    contents.append(&mut Vec::from("\n[]".as_bytes())); // improved ergonomics of trailing comments
    let tokens: Vec<AnyValue> = BytesBinarySource::new(&contents)
        .text::<AnyValue, _>(ViaCodec::new(NoEmbeddedDomainCodec))
        .configured(true)
        .collect::<Result<Vec<_>, _>>()?;
    match script::Parser::new(&tokens).parse_top("config") {
        Ok(Some(i)) => Ok(Some(t.facet(|t| {
            tracing::debug!("Instructions for file {:?}: {:#?}", &env.path, &i);
            env.safe_eval(t, &i);
            Ok(())
        }).expect("Successful facet startup"))),
        Ok(None) => Ok(None),
        Err(errors) => {
            for e in errors {
                tracing::error!(path = ?env.path, message = %e);
            }
            Ok(None)
        }
    }
}

fn process_path(
    t: &mut Activation,
    env: script::Env,
) -> io::Result<Option<FacetId>> {
    match fs::metadata(&env.path) {
        Ok(md) => if md.is_file() {
            process_existing_file(t, env)
        } else {
            Ok(None)
        }
        Err(e) => if e.kind() != io::ErrorKind::NotFound {
            Err(e)?
        } else {
            Ok(None)
        }
    }
}

fn is_hidden(path: &PathBuf) -> bool {
    match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.starts_with("."),
        None => true, // ?
    }
}

fn should_process(path: &PathBuf) -> bool {
    path.file_name().and_then(|n| n.to_str()).map(|n| n.ends_with(".pr")).unwrap_or(false)
}

fn scan_file(
    t: &mut Activation,
    path_state: &mut Map<PathBuf, FacetId>,
    env: script::Env,
) -> bool {
    let path = env.path.clone();
    if is_hidden(&path) || !should_process(&path) {
        return true;
    }
    tracing::trace!("scan_file: scanning {:?}", &path);
    match process_path(t, env) {
        Ok(maybe_facet_id) => {
            if let Some(facet_id) = maybe_facet_id {
                tracing::info!("scan_file: processed {:?}", &path);
                path_state.insert(path, facet_id);
            }
            true
        },
        Err(e) => {
            tracing::error!("scan_file: {:?}: {:?}", &path, e);
            false
        }
    }
}

fn initial_scan(
    t: &mut Activation,
    path_state: &mut Map<PathBuf, FacetId>,
    config_ds: &Arc<Cap>,
    env: script::Env,
) {
    if is_hidden(&env.path) {
        return;
    }
    match fs::metadata(&env.path) {
        Ok(md) => if md.is_file() {
            scan_file(t, path_state, env);
        } else {
            match fs::read_dir(&env.path) {
                Ok(unsorted_entries) => {
                    let mut entries: Vec<fs::DirEntry> = Vec::new();
                    for er in unsorted_entries {
                        match er {
                            Ok(e) =>
                                entries.push(e),
                            Err(e) =>
                                tracing::warn!(
                                    "initial_scan: transient during scan of {:?}: {:?}", &env.path, e),
                        }
                    }
                    entries.sort_by_key(|e| e.file_name());
                    for e in entries {
                        initial_scan(t, path_state, config_ds, env.clone_with_path(e.path()));
                    }
                }
                Err(e) => tracing::warn!("initial_scan: enumerating {:?}: {:?}", &env.path, e),
            }
        },
        Err(e) => tracing::warn!("initial_scan: `stat`ing {:?}: {:?}", &env.path, e),
    }
}

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    spec: internal_services::ConfigWatcher,
) -> ActorResult {
    lifecycle::terminate_on_service_restart(t, &config_ds, &spec);

    let path = fs::canonicalize(spec.path.clone())?;
    let env = script::Env::new(path, spec.env.0.clone());

    tracing::info!(?env);
    let (tx, rx) = channel();

    let mut watcher = watcher(tx, Duration::from_millis(100)).map_err(convert_notify_error)?;
    watcher.watch(&env.path, RecursiveMode::Recursive).map_err(convert_notify_error)?;

    let facet = t.facet.clone();
    let trace_collector = t.trace_collector();
    let span = tracing::Span::current();
    thread::spawn(move || {
        let _entry = span.enter();

        let mut path_state: Map<PathBuf, FacetId> = Map::new();

        {
            let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("initial_scan"));
            let account = Account::new(Some(AnyValue::symbol("initial_scan")), trace_collector.clone());
            if !facet.activate(
                &account, cause, |t| {
                    initial_scan(t, &mut path_state, &config_ds, env.clone());
                    config_ds.assert(t, language(), &lifecycle::ready(&spec));
                    Ok(())
                })
            {
                return;
            }
        }
        tracing::trace!("initial_scan complete");

        let mut rescan = |paths: Vec<PathBuf>| {
            let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("rescan"));
            let account = Account::new(Some(AnyValue::symbol("rescan")), trace_collector.clone());
            facet.activate(&account, cause, |t| {
                let mut to_stop = Vec::new();
                for path in paths.into_iter() {
                    let maybe_facet_id = path_state.remove(&path);
                    let new_content_ok =
                        scan_file(t, &mut path_state, env.clone_with_path(path.clone()));
                    if let Some(old_facet_id) = maybe_facet_id {
                        if new_content_ok {
                            to_stop.push(old_facet_id);
                        } else {
                            path_state.insert(path, old_facet_id);
                        }
                    }
                }
                for facet_id in to_stop.into_iter() {
                    t.stop_facet(facet_id);
                }
                Ok(())
            })
        };

        while let Ok(event) = rx.recv() {
            tracing::trace!("notification: {:?}", &event);
            let keep_running = match event {
                DebouncedEvent::NoticeWrite(_p) |
                DebouncedEvent::NoticeRemove(_p) =>
                    true,
                DebouncedEvent::Create(p) |
                DebouncedEvent::Write(p) |
                DebouncedEvent::Chmod(p) |
                DebouncedEvent::Remove(p) =>
                    rescan(vec![p]),
                DebouncedEvent::Rename(p, q) =>
                    rescan(vec![p, q]),
                _ => {
                    tracing::info!("{:?}", event);
                    true
                }
            };
            if !keep_running { break; }
        }

        {
            let cause = trace_collector.as_ref().map(|_| trace::TurnCause::external("termination"));
            let account = Account::new(Some(AnyValue::symbol("termination")), trace_collector);
            facet.activate(&account, cause, |t| {
                tracing::trace!("linked thread terminating associated facet");
                Ok(t.stop())
            });
        }

        tracing::trace!("linked thread done");
    });

    t.linked_task(Some(AnyValue::symbol("cancel-wait")), async move {
        future::pending::<()>().await;
        drop(watcher);
        Ok(LinkedTaskTermination::KeepFacet)
    });

    Ok(())
}
