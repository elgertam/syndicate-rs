use std::sync::Arc;

use crate::schemas::dataspace_patterns::*;

use super::language;

use preserves::CompoundClass;
use preserves::Map;
use preserves::Record;
use preserves::Value;
use preserves::ValueClass;
use preserves::signed_integer::OutOfRange;
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
                self.const_values.push(language().unparse(value));
            }
        }
    }
}

impl Pattern<_Ptr> {
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

    fn run_seq<F: Fn(usize) -> Option<_Any>>(&mut self, entries: &Map<_Any, Pattern<_Ptr>>, values: F) -> bool {
        for (k, p) in entries {
            match k.as_usize() {
                None => return false,
                Some(Err(OutOfRange(_))) => return false,
                Some(Ok(i)) => match values(i) {
                    Some(v) => if !self.run(p, &v) {
                        return false;
                    },
                    None => return false,
                },
            }
        }
        true
    }

    fn run(&mut self, pattern: &Pattern<_Ptr>, value: &_Any) -> bool {
        match pattern {
            Pattern::Discard => true,
            Pattern::Bind { pattern } => {
                self.captures.push(value.clone());
                self.run(&**pattern, value)
            }
            Pattern::Lit { value: expected } => value == &language().unparse(expected),
            Pattern::Group { type_, entries } => match type_ {
                GroupType::Rec { label } =>
                    value.is_record() &&
                    &value.label() == label &&
                    self.run_seq(entries, |i| (i < value.len()).then(|| value.index(i))),
                GroupType::Arr =>
                    value.is_sequence() &&
                    self.run_seq(entries, |i| (i < value.len()).then(|| value.index(i))),
                GroupType::Dict =>
                    value.is_dictionary() && {
                        for (k, p) in entries {
                            if !value.get(k).map(|v| self.run(p, &v)).unwrap_or(false) {
                                return false;
                            }
                        }
                        true
                    },
            }
        }
    }
}

pub fn lift_literal(v: &_Any) -> Pattern {
    match v.value_class() {
        ValueClass::Compound(CompoundClass::Record) => Pattern::Group {
            type_: GroupType::Rec { label: v.label() },
            entries: v.iter().enumerate()
                .map(|(i, v)| (_Any::new(i), lift_literal(&v)))
                .collect(),
        },
        ValueClass::Compound(CompoundClass::Sequence) => Pattern::Group {
            type_: GroupType::Arr,
            entries: v.iter().enumerate()
                .map(|(i, v)| (_Any::new(i), lift_literal(&v)))
                .collect(),
        },
        ValueClass::Compound(CompoundClass::Set) => panic!("Cannot express literal set in pattern"),
        ValueClass::Compound(CompoundClass::Dictionary) => Pattern::Group {
            type_: GroupType::Dict,
            entries: v.entries()
                .map(|(k, v)| (k, lift_literal(&v)))
                .collect(),
        },
        _other => Pattern::Lit {
            value: language().parse(v).expect("Failed converting non-compound datum to AnyAtom"),
        },
    }
}

const DISCARD: Pattern = Pattern::Discard;

pub fn pattern_seq_from_dictionary(entries: &Map<_Any, Pattern>) -> Option<Vec<&Pattern>> {
    let mut max_k: Option<usize> = None;
    for k in entries.keys() {
        max_k = max_k.max(Some(k.as_usize()?.ok()?));
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
        Pattern::Group { type_, entries } => match type_ {
            GroupType::Rec { label } =>
                Some(Value::new(Record::_from_vec(drop_literal_entries_seq(vec![label.clone()], entries)?))),
            GroupType::Arr =>
                Some(Value::new(drop_literal_entries_seq(vec![], entries)?)),
            GroupType::Dict =>
            Some(Value::new(entries.iter()
                .map(|(k, p)| Some((k.clone(), drop_literal(p)?)))
                .collect::<Option<Map<_Any, _Any>>>()?)),
        },
        Pattern::Lit { value } => Some(language().unparse(value)),
        _ => None,
    }
}
