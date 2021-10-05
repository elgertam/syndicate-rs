use parking_lot::RwLock;

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use syndicate::value::IOValue;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::Value;

use termion::raw::IntoRawMode;

use tracing::Event;
use tracing::Level;
use tracing::Metadata;
use tracing::field::Field;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing::span::Record;

use tracing_subscriber::prelude::*;
use tracing_subscriber::layer::Context;

use tui::Terminal;
use tui::backend::TermionBackend;

#[derive(Debug, Clone, Default)]
struct FieldsSnapshot(Map<&'static str, IOValue>);

#[derive(Debug, Clone)]
struct EventSnapshot {
    spans: Vec<(&'static Metadata<'static>, FieldsSnapshot)>,
    event_metadata: &'static Metadata<'static>,
    event_fields: FieldsSnapshot,
}

struct LogCollector {
    dirty: AtomicBool,
    current_level: Level,
    minimum_level: Level,
    history: RwLock<VecDeque<EventSnapshot>>,
    history_limit: usize,
}

fn write_fields(f: &mut std::fmt::Formatter, fs: &Map<&'static str, IOValue>) -> std::fmt::Result {
    for (k, v) in fs.iter() {
        if let Some(s) = v.value().as_string() {
            write!(f, "{}={} ", k, s)?;
        } else {
            write!(f, "{}={:?} ", k, v)?;
        }
    }
    Ok(())
}

impl std::fmt::Display for EventSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut fs = self.event_fields.0.clone();
        if let Some(m) = fs.remove("message") {
            match m.value().as_string() {
                Some(s) => write!(f, "{} ", s)?,
                None => { fs.insert("message", m); }
            }
        }
        write_fields(f, &fs)?;
        write!(f, "at {}:{}\n",
               self.event_metadata.file().unwrap_or("-"),
               self.event_metadata.line().unwrap_or(0))?;
        for s in self.spans.iter() {
            let (m, fs) = s;
            write!(f, " - in {} ", m.name())?;
            write_fields(f, &fs.0)?;
            write!(f, "at {}:{}\n", m.file().unwrap_or("-"), m.line().unwrap_or(0))?;
        }
        Ok(())
    }
}

impl FieldsSnapshot {
    fn push(&mut self, field: &Field, v: IOValue) {
        let name = field.name();
        match self.0.remove(name) {
            None => {
                self.0.insert(name, v);
            }
            Some(o) => match o.value().as_sequence() {
                Some(vs) => {
                    let mut vs = vs.clone();
                    vs.push(v);
                    self.0.insert(name, IOValue::new(vs));
                },
                None => {
                    self.0.insert(name, IOValue::new(vec![o, v]));
                }
            }
        }
    }
}

impl tracing::field::Visit for FieldsSnapshot {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push(field, IOValue::new(format!("{:?}", value)));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, IOValue::new(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, IOValue::new(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field, IOValue::new(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, IOValue::new(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, IOValue::new(value));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let mut r = Value::simple_record("error", 1);
        r.fields_vec_mut().push(IOValue::new(format!("{}", value)));
        let r = r.finish();
        self.push(field, r.wrap());
    }
}

impl LogCollector {
    fn new() -> Self {
        LogCollector {
            dirty: AtomicBool::new(false),
            current_level: Level::INFO,
            minimum_level: Level::TRACE,
            history: Default::default(),
            history_limit: 10000,
        }
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + std::fmt::Debug>
    tracing_subscriber::Layer<S> for LogCollector
where for<'a> <S as tracing_subscriber::registry::LookupSpan<'a>>::Data: std::fmt::Debug
{
    fn enabled(&self, metadata: &Metadata, _ctx: Context<S>) -> bool {
        metadata.level() <= &self.minimum_level
    }

    fn new_span(&self, attrs: &Attributes, id: &Id, ctx: Context<S>) {
        if let Some(s) = ctx.span(id) {
            let mut snap = FieldsSnapshot::default();
            attrs.record(&mut snap);
            s.extensions_mut().insert(snap);
        }
    }

    fn on_record(&self, span: &Id, values: &Record, ctx: Context<S>) {
        if let Some(s) = ctx.span(span) {
            if let Some(snap) = s.extensions_mut().get_mut::<FieldsSnapshot>() {
                values.record(snap);
            }
        }
    }

    fn on_event(&self, event: &Event, ctx: Context<S>) {
        if event.metadata().level() > &self.current_level {
            return;
        }

        let mut snap = EventSnapshot {
            spans: Vec::new(),
            event_metadata: event.metadata(),
            event_fields: Default::default(),
        };
        event.record(&mut snap.event_fields);

        if let Some(scope) = ctx.event_scope(event) {
            for s in scope.from_root() {
                snap.spans.push((
                    s.metadata(),
                    s.extensions().get::<FieldsSnapshot>().cloned().unwrap_or_else(FieldsSnapshot::default)
                ));
            }
        }

        println!("{}", &snap);

        let mut history = self.history.write();
        history.push_front(snap);
        history.truncate(self.history_limit);
        self.dirty.store(true, Ordering::Relaxed);
    }
}

pub fn start() -> io::Result<()> {
    let stdout = io::stdout().into_raw_mode()?;
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let subscriber = tracing_subscriber::Registry::default()
        .with(LogCollector::new());
    tracing::subscriber::set_global_default(subscriber)
        .expect("Could not set UI global subscriber");

    Ok(())
}
