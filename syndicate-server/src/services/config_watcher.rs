use notify::DebouncedEvent;
use notify::Watcher;
use notify::RecursiveMode;
use notify::watcher;

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
use syndicate::during;
use syndicate::enclose;
use syndicate::schemas::dataspace;
use syndicate::schemas::dataspace_patterns as P;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::value::BinarySource;
use syndicate::value::IOBinarySource;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::NoEmbeddedDomainCodec;
use syndicate::value::Reader;
use syndicate::value::Record;
use syndicate::value::Set;
use syndicate::value::Value;
use syndicate::value::ViaCodec;
use syndicate::value::signed_integer::SignedInteger;

use crate::language::language;
use crate::lifecycle;
use crate::schemas::internal_services;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(
            t, config_ds, language(), <run-service $spec: internal_services::ConfigWatcher>, |t| {
                Supervisor::start(
                    t,
                    syndicate::name!(parent: None, "config", spec = ?spec),
                    SupervisorConfiguration::default(),
                    enclose!((config_ds, spec) lifecycle::updater(config_ds, spec)),
                    enclose!((config_ds, root_ds) move |t|
                             enclose!((config_ds, root_ds, spec) run(t, config_ds, root_ds, spec))))
            }))
    });
}

fn convert_notify_error(e: notify::Error) -> Error {
    syndicate::error::error(&format!("Notify error: {:?}", e), AnyValue::new(false))
}

#[derive(Debug, Clone)]
enum Instruction {
    Assert {
        target: String,
        template: AnyValue,
    },
    React {
        target: String,
        pattern_template: AnyValue,
        body: Box<Instruction>,
    },
    Sequence {
        instructions: Vec<Instruction>,
    },
}

#[derive(Debug)]
enum Symbolic {
    Reference(String),
    Binder(String),
    Discard,
    Literal(String),
}

fn analyze(s: &str) -> Symbolic {
    if s == "_" {
        Symbolic::Discard
    } else if s.starts_with("?") {
        Symbolic::Binder(s[1..].to_owned())
    } else if s.starts_with("$") {
        Symbolic::Reference(s[1..].to_owned())
    } else if s.starts_with("=") {
        Symbolic::Literal(s[1..].to_owned())
    } else {
        Symbolic::Literal(s.to_owned())
    }
}

fn bad_instruction(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn discard() -> P::Pattern {
    P::Pattern::DDiscard(Box::new(P::DDiscard))
}

#[derive(Debug)]
struct PatternInstantiator<'e> {
    env: &'e Env,
    binding_names: Vec<String>,
}

#[derive(Debug, Clone)]
struct Env(Map<String, AnyValue>);

impl<'e> PatternInstantiator<'e> {
    fn instantiate_pattern(&mut self, template: &AnyValue) -> io::Result<P::Pattern> {
        Ok(match template.value() {
            Value::Boolean(_) |
            Value::Float(_) |
            Value::Double(_) |
            Value::SignedInteger(_) |
            Value::String(_) |
            Value::ByteString(_) |
            Value::Embedded(_) =>
                P::Pattern::DLit(Box::new(P::DLit { value: template.clone() })),

            Value::Symbol(s) => match analyze(s) {
                Symbolic::Discard => discard(),
                Symbolic::Binder(s) => {
                    self.binding_names.push(s);
                    P::Pattern::DBind(Box::new(P::DBind { pattern: discard() }))
                }
                Symbolic::Reference(s) =>
                    P::Pattern::DLit(Box::new(P::DLit {
                        value: self.env.0.get(&s)
                            .ok_or_else(|| bad_instruction(
                                format!("Undefined pattern-template variable: {:?}", template)))?
                            .clone(),
                    })),
                Symbolic::Literal(s) =>
                    P::Pattern::DLit(Box::new(P::DLit {
                        value: Value::Symbol(s).wrap(),
                    })),
            },

            Value::Record(r) =>
                P::Pattern::DCompound(Box::new(P::DCompound::Rec {
                    ctor: Box::new(P::CRec {
                        label: r.label().clone(),
                        arity: r.fields().len().into(),
                    }),
                    members: r.fields().iter().enumerate()
                        .map(|(i, p)| Ok((i.into(), self.instantiate_pattern(p)?)))
                        .filter(|e| discard() != e.as_ref().unwrap().1)
                        .collect::<io::Result<Map<SignedInteger, P::Pattern>>>()?,
                })),
            Value::Sequence(v) =>
                P::Pattern::DCompound(Box::new(P::DCompound::Arr {
                    ctor: Box::new(P::CArr {
                        arity: v.len().into(),
                    }),
                    members: v.iter().enumerate()
                        .map(|(i, p)| Ok((i.into(), self.instantiate_pattern(p)?)))
                        .filter(|e| discard() != e.as_ref().unwrap().1)
                        .collect::<io::Result<Map<SignedInteger, P::Pattern>>>()?,
                })),
            Value::Set(_) =>
                Err(bad_instruction(format!("Sets not permitted in patterns: {:?}", template)))?,
            Value::Dictionary(v) =>
                P::Pattern::DCompound(Box::new(P::DCompound::Dict {
                    ctor: Box::new(P::CDict),
                    members: v.iter()
                        .map(|(a, b)| Ok((a.clone(), self.instantiate_pattern(b)?)))
                        .collect::<io::Result<Map<AnyValue, P::Pattern>>>()?,
                })),
        })
    }
}

impl Env {
    fn lookup_target(&self, target: &String) -> io::Result<Arc<Cap>> {
        Ok(self.0.get(target)
           .ok_or_else(|| bad_instruction(format!("Undefined target variable: {:?}", target)))?
           .value().to_embedded()?.clone())
    }

    fn instantiate_value(&self, template: &AnyValue) -> io::Result<AnyValue> {
        Ok(match template.value() {
            Value::Boolean(_) |
            Value::Float(_) |
            Value::Double(_) |
            Value::SignedInteger(_) |
            Value::String(_) |
            Value::ByteString(_) |
            Value::Embedded(_) =>
                template.clone(),

            Value::Symbol(s) => match analyze(s) {
                Symbolic::Binder(_) | Symbolic::Discard =>
                    Err(bad_instruction(
                        format!("Invalid use of wildcard in template: {:?}", template)))?,
                Symbolic::Reference(s) =>
                    self.0.get(&s).ok_or_else(|| bad_instruction(
                        format!("Undefined template variable: {:?}", template)))?.clone(),
                Symbolic::Literal(s) =>
                    Value::Symbol(s).wrap(),
            },

            Value::Record(r) =>
                Value::Record(Record(r.fields_vec().iter().map(|a| self.instantiate_value(a))
                                     .collect::<Result<Vec<_>, _>>()?)).wrap(),
            Value::Sequence(v) =>
                Value::Sequence(v.iter().map(|a| self.instantiate_value(a))
                                .collect::<Result<Vec<_>, _>>()?).wrap(),
            Value::Set(v) =>
                Value::Set(v.iter().map(|a| self.instantiate_value(a))
                           .collect::<Result<Set<_>, _>>()?).wrap(),
            Value::Dictionary(v) =>
                Value::Dictionary(v.iter().map(|(a,b)| Ok((self.instantiate_value(a)?,
                                                           self.instantiate_value(b)?)))
                                  .collect::<io::Result<Map<_, _>>>()?).wrap(),
        })
    }
}

impl Instruction {
    fn eval(&self, t: &mut Activation, env: &Env) -> io::Result<()> {
        match self {
            Instruction::Assert { target, template } => {
                env.lookup_target(target)?.assert(t, &(), &env.instantiate_value(template)?);
            }
            Instruction::React { target, pattern_template, body } => {
                let mut inst = PatternInstantiator {
                    env,
                    binding_names: Vec::new(),
                };
                let pattern = inst.instantiate_pattern(pattern_template)?;
                let binding_names = inst.binding_names;
                let observer = during::entity(env.clone())
                    .on_asserted_facet(enclose!(
                        (binding_names, body) move |env, t, captures: AnyValue| {
                            if let Some(captures) = captures.value_owned().into_sequence() {
                                let mut env = env.clone();
                                for (k, v) in binding_names.iter().zip(captures) {
                                    env.0.insert(k.clone(), v);
                                }
                                body.eval(t, &env)?;
                            }
                            Ok(())
                        }))
                    .create_cap(t);
                env.lookup_target(target)?.assert(t, language(), &dataspace::Observe {
                    pattern,
                    observer,
                });
            }
            Instruction::Sequence { instructions } => {
                for i in instructions {
                    i.eval(t, env)?;
                }
            }
        }
        Ok(())
    }

    fn parse<'t>(target: &str, tokens: &'t [AnyValue]) -> io::Result<Option<(Instruction, &'t [AnyValue])>> {
        if tokens.len() == 0 {
            return Ok(None);
        }

        if tokens[0].value().is_record() || tokens[0].value().is_dictionary() {
            return Ok(Some((Instruction::Assert {
                target: target.to_owned(),
                template: tokens[0].clone(),
            }, &tokens[1..])));
        }

        if let Some(tokens) = tokens[0].value().as_sequence() {
            return Ok(Some((Instruction::Sequence {
                instructions: Instruction::parse_all(target, tokens)?,
            }, &tokens[1..])));
        }

        if let Some(s) = tokens[0].value().as_symbol() {
            match analyze(s) {
                Symbolic::Binder(s) =>
                    if s.len() == 0 {
                        if tokens.len() == 1 {
                            Err(bad_instruction(format!("Missing pattern and instruction in react")))?;
                        } else {
                            let pattern_template = tokens[1].clone();
                            match Instruction::parse(target, &tokens[2..])? {
                                None =>
                                    Err(bad_instruction(format!("Missing instruction in react with pattern {:?}", tokens[1])))?,
                                Some((body, tokens)) =>
                                    return Ok(Some((Instruction::React {
                                        target: target.to_owned(),
                                        pattern_template,
                                        body: Box::new(body),
                                    }, tokens))),
                            }
                        }
                    } else {
                        Err(bad_instruction(format!("Invalid use of pattern binder in target: {:?}", tokens[0])))?;
                    },
                Symbolic::Discard =>
                    Err(bad_instruction(format!("Invalid use of discard in target: {:?}", tokens[0])))?,
                Symbolic::Reference(s) =>
                    if tokens.len() == 1 {
                        Err(bad_instruction(format!("Missing instruction after retarget: {:?}", tokens[0])))?;
                    } else if tokens[1].value().is_symbol() {
                        Err(bad_instruction(format!("Two retargets in a row: {:?} {:?}", tokens[0], tokens[1])))?;
                    } else {
                        return Instruction::parse(&s, &tokens[1..]);
                    },
                Symbolic::Literal(_) =>
                    /* fall through */ (),
            }
        }

        Err(bad_instruction(format!("Invalid token: {:?}", tokens[0])))?
    }

    fn parse_all(target: &str, mut tokens: &[AnyValue]) -> io::Result<Vec<Instruction>> {
        let mut instructions = Vec::new();
        while let Some((i, more)) = Instruction::parse(target, tokens)? {
            instructions.push(i);
            tokens = more;
        }
        Ok(instructions)
    }
}

fn process_existing_file(
    t: &mut Activation,
    config_ds: &Arc<Cap>,
    root_ds: &Arc<Cap>,
    path: &PathBuf,
) -> io::Result<Option<FacetId>> {
    let tokens: Vec<AnyValue> = IOBinarySource::new(fs::File::open(path)?)
        .text::<AnyValue, _>(ViaCodec::new(NoEmbeddedDomainCodec))
        .configured(true)
        .collect::<Result<Vec<_>, _>>()?;
    let instructions = Instruction::parse_all("config", &tokens)?;
    if instructions.is_empty() {
        Ok(None)
    } else {
        let mut env = Map::new();
        env.insert("config".to_owned(), AnyValue::domain(config_ds.clone()));
        env.insert("root".to_owned(), AnyValue::domain(root_ds.clone()));
        match t.facet(|t| Ok(Instruction::Sequence { instructions }.eval(t, &Env(env))?)) {
            Ok(facet_id) => Ok(Some(facet_id)),
            Err(error) => {
                tracing::error!(?path, ?error);
                Ok(None)
            }
        }
    }
}

fn process_path(
    t: &mut Activation,
    config_ds: &Arc<Cap>,
    root_ds: &Arc<Cap>,
    path: &PathBuf,
) -> io::Result<Option<FacetId>> {
    match fs::metadata(path) {
        Ok(md) => if md.is_file() {
            process_existing_file(t, config_ds, root_ds, path)
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

fn scan_file(
    t: &mut Activation,
    path_state: &mut Map<PathBuf, FacetId>,
    config_ds: &Arc<Cap>,
    root_ds: &Arc<Cap>,
    path: &PathBuf,
) -> bool {
    if is_hidden(path) {
        return true;
    }
    tracing::info!("scan_file: {:?}", path);
    match process_path(t, config_ds, root_ds, path) {
        Ok(maybe_facet_id) => {
            if let Some(facet_id) = maybe_facet_id {
                path_state.insert(path.clone(), facet_id);
            }
            true
        },
        Err(e) => {
            tracing::warn!("scan_file: {:?}: {:?}", path, e);
            false
        }
    }
}

fn initial_scan(
    t: &mut Activation,
    path_state: &mut Map<PathBuf, FacetId>,
    config_ds: &Arc<Cap>,
    root_ds: &Arc<Cap>,
    path: &PathBuf,
) {
    if is_hidden(path) {
        return;
    }
    match fs::metadata(path) {
        Ok(md) => if md.is_file() {
            scan_file(t, path_state, config_ds, root_ds, path);
        } else {
            match fs::read_dir(path) {
                Ok(entries) => for er in entries {
                    match er {
                        Ok(e) => initial_scan(t, path_state, config_ds, root_ds, &e.path()),
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

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    root_ds: Arc<Cap>,
    spec: internal_services::ConfigWatcher,
) -> ActorResult {
    let path = fs::canonicalize(spec.path.clone())?;

    tracing::info!("watching {:?}", &path);
    let (tx, rx) = channel();

    let mut watcher = watcher(tx, Duration::from_millis(100)).map_err(convert_notify_error)?;
    watcher.watch(&path, RecursiveMode::Recursive).map_err(convert_notify_error)?;

    let facet = t.facet.clone();
    thread::spawn(move || {
        let mut path_state: Map<PathBuf, FacetId> = Map::new();

        {
            let root_path = path.clone().into();
            facet.activate(Account::new(syndicate::name!("initial_scan")), |t| {
                initial_scan(t, &mut path_state, &config_ds, &root_ds, &root_path);
                config_ds.assert(t, language(), &lifecycle::ready(&spec));
                Ok(())
            }).unwrap();
            tracing::trace!("initial_scan complete");
        }

        let mut rescan = |paths: Vec<PathBuf>| {
            facet.activate(Account::new(syndicate::name!("rescan")), |t| {
                let mut to_stop = Vec::new();
                for path in paths.into_iter() {
                    let maybe_facet_id = path_state.remove(&path);
                    let new_content_ok = scan_file(t, &mut path_state, &config_ds, &root_ds, &path);
                    if let Some(old_facet_id) = maybe_facet_id {
                        if new_content_ok {
                            to_stop.push(old_facet_id);
                        } else {
                            path_state.insert(path, old_facet_id);
                        }
                    }
                }
                for facet_id in to_stop.into_iter() {
                    t.stop_facet(facet_id)?;
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

        let _ = facet.activate(Account::new(syndicate::name!("termination")), |t| {
            tracing::trace!("linked thread terminating associated facet");
            t.stop()
        });

        tracing::trace!("linked thread done");
    });

    t.linked_task(syndicate::name!("cancel-wait"), async move {
        future::pending::<()>().await;
        drop(watcher);
        Ok(LinkedTaskTermination::KeepFacet)
    });

    Ok(())
}
