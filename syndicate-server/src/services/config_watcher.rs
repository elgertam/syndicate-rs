use notify::DebouncedEvent;
use notify::Watcher;
use notify::RecursiveMode;
use notify::watcher;

use std::convert::TryFrom;
use std::fs;
use std::future;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use syndicate::actor::*;
use syndicate::convert::*;
use syndicate::during::entity;
use syndicate::schemas::dataspace::Observe;
use syndicate::value::BinarySource;
use syndicate::value::IOBinarySource;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::NoEmbeddedDomainCodec;
use syndicate::value::Reader;
use syndicate::value::Set;
use syndicate::value::ViaCodec;

use crate::schemas::internal_services;

pub fn on_demand(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        let monitor = entity(())
            .on_asserted_facet({
                let ds = Arc::clone(&ds);
                move |_, t, captures| {
                    let ds = Arc::clone(&ds);
                    t.spawn_link(syndicate::name!(parent: None, "config", spec = ?captures),
                                 |t| run(t, ds, captures));
                    Ok(())
                }
            })
            .create_cap(t);
        ds.assert(t, &Observe {
            pattern: syndicate_macros::pattern!("<require-service <$ <config-watcher _>>>"),
            observer: monitor,
        });
        Ok(())
    })
}

fn convert_notify_error(e: notify::Error) -> syndicate::error::Error {
    syndicate::error::error(&format!("Notify error: {:?}", e), AnyValue::new(false))
}

fn assertions_at_existing_file(t: &mut Activation, ds: &Arc<Cap>, path: &PathBuf) -> io::Result<Set<Handle>> {
    let mut handles = Set::new();
    let fh = fs::File::open(path)?;
    let mut src = IOBinarySource::new(fh);
    let mut r = src.text::<_, AnyValue, _>(ViaCodec::new(NoEmbeddedDomainCodec));
    while let Some(value) = Reader::<_, AnyValue>::next(&mut r, true)? {
        if let Some(handle) = ds.assert(t, value.clone()) {
            tracing::debug!("asserted {:?} -> {:?}", value, handle);
            handles.insert(handle);
        }
    }
    Ok(handles)
}

fn assertions_at_path(t: &mut Activation, ds: &Arc<Cap>, path: &PathBuf) -> io::Result<Set<Handle>> {
    match fs::metadata(path) {
        Ok(md) => if md.is_file() {
            assertions_at_existing_file(t, ds, path)
        } else {
            Ok(Set::new())
        }
        Err(e) => if e.kind() != io::ErrorKind::NotFound {
            Err(e)?
        } else {
            Ok(Set::new())
        }
    }
}

fn is_hidden(path: &PathBuf) -> bool {
    match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n.starts_with("."),
        None => true, // ?
    }
}

fn scan_file(
    t: &mut Activation,
    path_state: &mut Map<PathBuf, Set<Handle>>,
    ds: &Arc<Cap>,
    path: &PathBuf,
) {
    if is_hidden(path) {
        return;
    }
    tracing::info!("scan_file: {:?}", path);
    match assertions_at_path(t, ds, path) {
        Ok(new_handles) => if !new_handles.is_empty() {
            path_state.insert(path.clone(), new_handles);
        },
        Err(e) => tracing::warn!("scan_file: {:?}: {:?}", path, e),
    }
}

fn initial_scan(
    t: &mut Activation,
    path_state: &mut Map<PathBuf, Set<Handle>>,
    ds: &Arc<Cap>,
    path: &PathBuf,
) {
    if is_hidden(path) {
        return;
    }
    match fs::metadata(path) {
        Ok(md) => if md.is_file() {
            scan_file(t, path_state, ds, path)
        } else {
            match fs::read_dir(path) {
                Ok(entries) => for er in entries {
                    match er {
                        Ok(e) => initial_scan(t, path_state, ds, &e.path()),
                        Err(e) => tracing::warn!(
                            "initial_scan: transient during scan of {:?}: {:?}", path, e),
                    }
                }
                Err(e) => tracing::warn!("initial_scan: enumerating {:?}: {:?}", path, e),
            }
        },
        Err(e) => tracing::warn!("initial_scan: `stat`ing {:?}: {:?}", path, e),
    }
}

fn run(t: &mut Activation, ds: Arc<Cap>, captures: AnyValue) -> ActorResult {
    let spec = internal_services::ConfigWatcher::try_from(&from_any_value(
        &captures.value().to_sequence()?[0])?)?;
    {
        let spec = from_io_value(&spec)?;
        ds.assert(t, syndicate_macros::template!("<service-running =spec>"));
    }
    let path = fs::canonicalize(spec.path)?;

    tracing::info!("watching {:?}", &path);
    let (tx, rx) = channel();

    let mut watcher = watcher(tx, Duration::from_secs(1)).map_err(convert_notify_error)?;
    watcher.watch(&path, RecursiveMode::Recursive).map_err(convert_notify_error)?;

    let facet = t.facet.clone();
    thread::spawn(move || {
        let mut path_state: Map<PathBuf, Set<Handle>> = Map::new();

        let account = Account::new(syndicate::name!("watcher"));

        {
            let root_path = path.clone().into();
            facet.activate(Arc::clone(&account), |t| {
                initial_scan(t, &mut path_state, &ds, &root_path);
                Ok(())
            }).unwrap();
            tracing::trace!("initial_scan complete");
        }

        let mut rescan = |paths: Vec<PathBuf>| {
            facet.activate(Arc::clone(&account), |t| {
                let mut to_retract = Set::new();
                for path in paths.into_iter() {
                    if let Some(handles) = path_state.remove(&path) {
                        to_retract.extend(handles.into_iter());
                    }
                    scan_file(t, &mut path_state, &ds, &path);
                }
                for h in to_retract.into_iter() {
                    tracing::debug!("retract {:?}", h);
                    t.retract(h);
                }
                Ok(())
            }).unwrap()
        };

        while let Ok(event) = rx.recv() {
            tracing::trace!("notification: {:?}", &event);
            match event {
                DebouncedEvent::NoticeWrite(_p) |
                DebouncedEvent::NoticeRemove(_p) =>
                    (),
                DebouncedEvent::Create(p) |
                DebouncedEvent::Write(p) |
                DebouncedEvent::Chmod(p) |
                DebouncedEvent::Remove(p) =>
                    rescan(vec![p]),
                DebouncedEvent::Rename(p, q) =>
                    rescan(vec![p, q]),
                _ =>
                    tracing::info!("{:?}", event),
            }
        }

        let _ = facet.activate(Arc::clone(&account), |t| {
            tracing::trace!("linked thread terminating associated facet");
            t.stop();
            Ok(())
        });

        tracing::trace!("linked thread done");
    });

    t.linked_task(syndicate::name!("cancel-wait"), async move {
        future::pending::<()>().await;
        drop(watcher);
        Ok(())
    });

    Ok(())
}
