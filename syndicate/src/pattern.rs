use std::sync::Arc;

use crate::schemas::dataspace_patterns::*;

use super::language;

use preserves::value::Map;
use preserves::value::NestedValue;
use preserves::value::Record;
use preserves::value::Value;
use preserves_schema::Codec;

pub type PathStep = _Any;
pub type Path = Vec<PathStep>;
pub type Paths = Vec<Path>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConstantPositions {
    pub with_values: Paths,
    pub required_to_exist: Paths,
}

struct Analyzer {
    pub const_paths: Paths,
    pub const_values: Vec<_Any>,
    pub checked_paths: Paths,
    pub capture_paths: Paths,
}

pub struct PatternAnalysis {
    pub const_positions: Arc<ConstantPositions>,
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
            checked_paths: Vec::new(),
            capture_paths: Vec::new(),
        };
        analyzer.walk(&mut Vec::new(), p);
        PatternAnalysis {
            const_positions: Arc::new(ConstantPositions {
                with_values: analyzer.const_paths,
                required_to_exist: analyzer.checked_paths,
            }),
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
            Pattern::Group { entries, .. } => {
                for (k, p) in entries {
                    self.walk_step(path, k.clone(), p)
                }
            }
            Pattern::Bind { pattern } => {
                self.capture_paths.push(path.clone());
                self.walk(path, &**pattern);
            }
            Pattern::Discard => {
                self.checked_paths.push(path.clone());
            }
            Pattern::Lit { value } => {
                self.const_paths.push(path.clone());
                self.const_values.push(language().unparse(&**value));
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

    fn run_seq<'a, F: 'a + Fn(usize) -> &'a _Any>(&mut self, entries: &Map<_Any, Pattern<_Any>>, values: F) -> bool {
        for (k, p) in entries {
            match k.value().as_usize() {
                None => return false,
                Some(i) => if !self.run(p, values(i)) {
                    return false;
                }
            }
        }
        true
    }

    fn run(&mut self, pattern: &Pattern<_Any>, value: &_Any) -> bool {
        match pattern {
            Pattern::Discard => true,
            Pattern::Bind { pattern } => {
                self.captures.push(value.clone());
                self.run(&**pattern, value)
            }
            Pattern::Lit { value: expected } => value == &language().unparse(&**expected),
            Pattern::Group { type_, entries } => match &**type_ {
                GroupType::Rec { label } => {
                    match value.value().as_record(None) {
                        None => false,
                        Some(r) =>
                            r.label() == label &&
                            self.run_seq(entries, |i| &r.fields()[i])
                    }
                }
                GroupType::Arr => {
                    match value.value().as_sequence() {
                        None => false,
                        Some(vs) =>
                            self.run_seq(entries, |i| &vs[i])
                    }
                }
                GroupType::Dict => {
                    match value.value().as_dictionary() {
                        None => false,
                        Some(actual_entries) => {
                            for (k, p) in entries {
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
        Value::Record(r) => Pattern::Group {
            type_: Box::new(GroupType::Rec { label: r.label().clone() }),
            entries: r.fields().iter().enumerate()
                .map(|(i, v)| (_Any::new(i), lift_literal(v)))
                .collect(),
        },
        Value::Sequence(items) => Pattern::Group {
            type_: Box::new(GroupType::Arr),
            entries: items.iter().enumerate()
                .map(|(i, v)| (_Any::new(i), lift_literal(v)))
                .collect(),
        },
        Value::Set(_members) => panic!("Cannot express literal set in pattern"),
        Value::Dictionary(entries) => Pattern::Group {
            type_: Box::new(GroupType::Dict),
            entries: entries.iter()
                .map(|(k, v)| (k.clone(), lift_literal(v)))
                .collect(),
        },
        _other => Pattern::Lit {
            value: Box::new(language().parse(v).expect("Non-compound datum can be converted to AnyAtom")),
        },
    }
}

const DISCARD: Pattern = Pattern::Discard;

pub fn pattern_seq_from_dictionary(entries: &Map<_Any, Pattern>) -> Option<Vec<&Pattern>> {
    let mut max_k: Option<usize> = None;
    for k in entries.keys() {
        max_k = max_k.max(Some(k.value().as_usize()?));
    }
    let mut seq = vec![];
    if let Some(max_k) = max_k {
        seq.reserve(max_k + 1);
        for i in 0..=max_k {
            seq.push(entries.get(&_Any::new(i)).unwrap_or(&DISCARD));
        }
    }
    return Some(seq);
}

fn drop_literal_entries_seq(mut seq: Vec<_Any>, entries: &Map<_Any, Pattern>) -> Option<Vec<_Any>> {
    for p in pattern_seq_from_dictionary(entries)?.into_iter() {
        seq.push(drop_literal(p)?);
    }
    Some(seq)
}

pub fn drop_literal(p: &Pattern) -> Option<_Any> {
    match p {
        Pattern::Group { type_, entries } => match &**type_ {
            GroupType::Rec { label } =>
                Some(Value::Record(Record(drop_literal_entries_seq(vec![label.clone()], entries)?)).wrap()),
            GroupType::Arr =>
                Some(Value::Sequence(drop_literal_entries_seq(vec![], entries)?).wrap()),
            GroupType::Dict =>
            Some(Value::Dictionary(entries.iter()
                .map(|(k, p)| Some((k.clone(), drop_literal(p)?)))
                .collect::<Option<Map<_Any, _Any>>>()?).wrap()),
        },
        Pattern::Lit { value } => Some(language().unparse(&**value)),
        _ => None,
    }
}
