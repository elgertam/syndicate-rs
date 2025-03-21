//! Records *describing* actions committed at the end of a turn and
//! events triggering the start of a turn. These are not the actions
//! or events themselves: they are reflective information on the
//! action of the system, enough to reconstruct interesting
//! projections of system activity.

pub use super::schemas::trace::*;

use preserves::IOValue;
use preserves::Writer;
use preserves_schema::Codec;

use super::actor::{self, AnyValue, Ref, Cap};
use super::language;

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

#[derive(Debug, Clone)]
pub struct TraceCollector {
    pub tx: UnboundedSender<TraceEntry>,
}

impl<M> From<&Ref<M>> for Target {
    fn from(v: &Ref<M>) -> Target {
        Target {
            actor: v.mailbox.actor_id.into(),
            facet: v.facet_id.into(),
            oid: Oid(AnyValue::new(v.oid())),
        }
    }
}

impl<M: std::fmt::Debug> From<&M> for AssertionDescription {
    default fn from(v: &M) -> Self {
        Self::Opaque { description: AnyValue::new(format!("{:?}", v)) }
    }
}

impl From<&AnyValue> for AssertionDescription {
    fn from(v: &AnyValue) -> Self {
        Self::Value { value: v.clone() }
    }
}

impl TraceCollector {
    pub fn record(&self, id: actor::ActorId, a: ActorActivation) {
        let _ = self.tx.send(TraceEntry {
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time after Unix epoch").as_secs_f64().into(),
            actor: id.into(),
            item: a,
        });
    }
}

impl TurnDescription {
    pub fn new(activation_id: u64, cause: TurnCause) -> Self {
        Self {
            id: TurnId(AnyValue::new(activation_id)),
            cause,
            actions: Vec::new(),
        }
    }

    pub fn record(&mut self, a: ActionDescription) {
        self.actions.push(a)
    }

    pub fn take(&mut self) -> Self {
        Self {
            id: self.id.clone(),
            cause: self.cause.clone(),
            actions: std::mem::take(&mut self.actions),
        }
    }
}

impl TurnCause {
    pub fn external(description: &str) -> Self {
        Self::External { description: AnyValue::new(description.to_string()) }
    }
}

struct CapEncoder;

impl preserves::DomainEncode<Arc<Cap>> for CapEncoder {
    fn encode_embedded(
        &mut self,
        w: &mut dyn Writer,
        d: &Arc<Cap>,
    ) -> std::io::Result<()> {
        w.write_string(&d.debug_str())
    }

    fn encode_value(&mut self, d: &Arc<Cap>) -> std::io::Result<IOValue> {
        Ok(IOValue::new(d.debug_str()))
    }
}

pub enum CollectorEvent {
    Event(TraceEntry),
    PeriodicFlush,
}

impl TraceCollector {
    pub fn new<F: 'static + Send + FnMut(CollectorEvent)>(mut f: F) -> TraceCollector {
        let (tx, mut rx) = unbounded_channel::<TraceEntry>();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                select! {
                    maybe_entry = rx.recv() => {
                        match maybe_entry {
                            None => break,
                            Some(entry) => {
                                tracing::trace!(?entry);
                                f(CollectorEvent::Event(entry));
                            }
                        }
                    },
                    _ = timer.tick() => f(CollectorEvent::PeriodicFlush),
                }
            }
        });
        TraceCollector { tx }
    }

    pub fn text<W: 'static + std::io::Write + Send>(w: W) -> TraceCollector {
        let mut writer = preserves::TextWriter::new(w);
        Self::new(move |event| match event {
            CollectorEvent::Event(entry) => {
                language().unparse(&entry).write(&mut writer, &mut CapEncoder)
                    .expect("failed to write TraceCollector entry");
                writer.borrow_write().write_all(b"\n")
                    .expect("failed to write TraceCollector newline");
            },
            CollectorEvent::PeriodicFlush =>
                writer.flush().expect("failed to flush TraceCollector output"),
        })
    }

    pub fn packed<W: 'static + std::io::Write + Send>(w: W) -> TraceCollector {
        let mut writer = preserves::PackedWriter::new(w);
        Self::new(move |event| match event {
            CollectorEvent::Event(entry) =>
                language().unparse(&entry).write(&mut writer, &mut CapEncoder)
                .expect("failed to write TraceCollector entry"),
            CollectorEvent::PeriodicFlush =>
                writer.flush().expect("failed to flush TraceCollector output"),
        })
    }
}

impl From<actor::Name> for Name {
    fn from(v: actor::Name) -> Name {
        match v {
            None => Name::Anonymous,
            Some(n) => Name::Named { name: n.clone() },
        }
    }
}

impl From<NonZeroU64> for ActorId {
    fn from(v: NonZeroU64) -> Self {
        ActorId(AnyValue::new(u64::from(v)))
    }
}

impl From<NonZeroU64> for FacetId {
    fn from(v: NonZeroU64) -> Self {
        FacetId(AnyValue::new(u64::from(v)))
    }
}
