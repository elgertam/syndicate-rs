use crate::schemas::dataspace_patterns::*;

use super::language;

use preserves::value::Map;
use preserves::value::NestedValue;
use preserves::value::Record;
use preserves::value::Value;
use preserves_schema::Codec;

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum PathStep {
    Index(usize),
    Key(_Any),
}

pub type Path = Vec<PathStep>;
pub type Paths = Vec<Path>;

struct Analyzer {
    pub const_paths: Paths,
    pub const_values: Vec<_Any>,
    pub capture_paths: Paths,
}

pub struct PatternAnalysis {
    pub const_paths: Paths,
    pub const_values: _Any,
    pub capture_paths: Paths,
}

struct PatternMatcher {
    captures: Vec<_Any>,
}

impl PatternAnalysis {
    pub fn new(p: &Pattern) -> Self {
        let mut analyzer = Analyzer {
            const_paths: Vec::new(),
            const_values: Vec::new(),
            capture_paths: Vec::new(),
        };
        analyzer.walk(&mut Vec::new(), p);
        PatternAnalysis {
            const_paths: analyzer.const_paths,
            const_values: _Any::new(analyzer.const_values),
            capture_paths: analyzer.capture_paths,
        }
    }
}

impl Analyzer {
    fn walk_step(&mut self, path: &mut Path, s: PathStep, p: &Pattern) {
        path.push(s);
        self.walk(path, p);
        path.pop();
    }

    fn walk(&mut self, path: &mut Path, p: &Pattern) {
        match p {
            Pattern::DCompound(b) => match &**b {
                DCompound::Rec { fields, .. } => {
                    for (i, p) in fields.iter().enumerate() {
                        self.walk_step(path, PathStep::Index(i), p);
                    }
                }
                DCompound::Arr { items, .. } => {
                    for (i, p) in items.iter().enumerate() {
                        self.walk_step(path, PathStep::Index(i), p);
                    }
                }
                DCompound::Dict { entries, .. } => {
                    for (k, p) in entries {
                        self.walk_step(path, PathStep::Key(k.clone()), p);
                    }
                }
            }
            Pattern::DBind(b) => {
                let DBind { pattern, .. } = &**b;
                self.capture_paths.push(path.clone());
                self.walk(path, pattern)
            }
            Pattern::DDiscard(_) =>
                (),
            Pattern::DLit(b) => {
                let DLit { value } = &**b;
                self.const_paths.push(path.clone());
                self.const_values.push(language().unparse(value));
            }
        }
    }
}

impl Pattern<_Any> {
    pub fn match_value(&self, value: &_Any) -> Option<Vec<_Any>> {
        let mut matcher = PatternMatcher::new();
        if matcher.run(self, value) {
            Some(matcher.captures)
        } else {
            None
        }
    }
}

impl PatternMatcher {
    fn new() -> Self {
        PatternMatcher {
            captures: Vec::new(),
        }
    }

    fn run(&mut self, pattern: &Pattern<_Any>, value: &_Any) -> bool {
        match pattern {
            Pattern::DDiscard(_) => true,
            Pattern::DBind(b) => {
                self.captures.push(value.clone());
                self.run(&b.pattern, value)
            }
            Pattern::DLit(b) => value == &language().unparse(&b.value),
            Pattern::DCompound(b) => match &**b {
                DCompound::Rec { label, fields } => {
                    match value.value().as_record(Some(fields.len())) {
                        None => false,
                        Some(r) => {
                            if r.label() != label {
                                return false;
                            }
                            for (i, p) in fields.iter().enumerate() {
                                if !self.run(p, &r.fields()[i]) {
                                    return false;
                                }
                            }
                            true
                        }
                    }
                }
                DCompound::Arr { items } => {
                    match value.value().as_sequence() {
                        None => false,
                        Some(vs) => {
                            if vs.len() != items.len() {
                                return false;
                            }
                            for (i, p) in items.iter().enumerate() {
                                if !self.run(p, &vs[i]) {
                                    return false;
                                }
                            }
                            true
                        }
                    }
                }
                DCompound::Dict { entries: expected_entries } => {
                    match value.value().as_dictionary() {
                        None => false,
                        Some(actual_entries) => {
                            for (k, p) in expected_entries.iter() {
                                if !actual_entries.get(k).map(|v| self.run(p, v)).unwrap_or(false) {
                                    return false;
                                }
                            }
                            true
                        }
                    }
                }
            }
        }
    }
}

pub fn lift_literal(v: &_Any) -> Pattern {
    match v.value() {
        Value::Record(r) => Pattern::DCompound(Box::new(DCompound::Rec {
            label: r.label().clone(),
            fields: r.fields().iter().map(lift_literal).collect(),
        })),
        Value::Sequence(items) => Pattern::DCompound(Box::new(DCompound::Arr {
            items: items.iter().map(lift_literal).collect(),
        })),
        Value::Set(_members) => panic!("Cannot express literal set in pattern"),
        Value::Dictionary(entries) => Pattern::DCompound(Box::new(DCompound::Dict {
            entries: entries.iter().map(|(k, v)| (k.clone(), lift_literal(v))).collect(),
        })),
        _other => Pattern::DLit(Box::new(DLit {
            value: language().parse(v).expect("Non-compound datum can be converted to AnyAtom"),
        })),
    }
}

pub fn drop_literal(p: &Pattern) -> Option<_Any> {
    match p {
        Pattern::DCompound(b) => match &**b {
            DCompound::Rec { label, fields } => {
                let mut r = vec![label.clone()];
                for f in fields.iter() {
                    r.push(drop_literal(f)?);
                }
                Some(Value::Record(Record(r)).wrap())
            }
            DCompound::Arr { items } =>
                Some(Value::Sequence(items.iter().map(drop_literal)
                                     .collect::<Option<Vec<_Any>>>()?).wrap()),
            DCompound::Dict { entries } =>
                Some(Value::Dictionary(entries.iter()
                                       .map(|(k, p)| Some((k.clone(), drop_literal(p)?)))
                                       .collect::<Option<Map<_Any, _Any>>>()?).wrap()),
        },
        Pattern::DLit(b) => Some(language().unparse(&b.value)),
        _ => None,
    }
}
