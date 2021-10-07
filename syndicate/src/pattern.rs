use crate::schemas::dataspace_patterns::*;

use preserves::value::NestedValue;

use std::convert::TryFrom;
use std::convert::TryInto;

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

struct PatternMatcher<N: NestedValue> {
    captures: Vec<N>,
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
                DCompound::Rec { members, .. } |
                DCompound::Arr { members, .. } => {
                    for (i, p) in members {
                        self.walk_step(path, PathStep::Index(usize::try_from(i).unwrap_or(0)), p);
                    }
                }
                DCompound::Dict { members, .. } => {
                    for (k, p) in members {
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
                self.const_values.push(value.clone());
            }
        }
    }
}

impl<N: NestedValue> Pattern<N> {
    pub fn match_value(&self, value: &N) -> Option<Vec<N>> {
        let mut matcher = PatternMatcher::new();
        if matcher.run(self, value) {
            Some(matcher.captures)
        } else {
            None
        }
    }
}

impl<N: NestedValue> PatternMatcher<N> {
    fn new() -> Self {
        PatternMatcher {
            captures: Vec::new(),
        }
    }

    fn run(&mut self, pattern: &Pattern<N>, value: &N) -> bool {
        match pattern {
            Pattern::DDiscard(_) => true,
            Pattern::DBind(b) => {
                self.captures.push(value.clone());
                self.run(&b.pattern, value)
            }
            Pattern::DLit(b) => value == &b.value,
            Pattern::DCompound(b) => match &**b {
                DCompound::Rec { ctor, members } => {
                    let arity = (&ctor.arity).try_into().expect("reasonable arity");
                    match value.value().as_record(Some(arity)) {
                        None => false,
                        Some(r) => {
                            for (i, p) in members.iter() {
                                let i: usize = i.try_into().expect("reasonable index");
                                if !self.run(p, &r.fields()[i]) {
                                    return false;
                                }
                            }
                            true
                        }
                    }
                }
                DCompound::Arr { ctor, members } => {
                    let arity: usize = (&ctor.arity).try_into().expect("reasonable arity");
                    match value.value().as_sequence() {
                        None => false,
                        Some(vs) => {
                            if vs.len() != arity {
                                return false;
                            }
                            for (i, p) in members.iter() {
                                let i: usize = i.try_into().expect("reasonable index");
                                if !self.run(p, &vs[i]) {
                                    return false;
                                }
                            }
                            true
                        }
                    }
                }
                DCompound::Dict { ctor: _, members } => {
                    match value.value().as_dictionary() {
                        None => false,
                        Some(entries) => {
                            for (k, p) in members.iter() {
                                if !entries.get(k).map(|v| self.run(p, v)).unwrap_or(false) {
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
