use crate::schemas::dataspace_patterns::*;

use preserves::value::NestedValue;

use std::convert::TryFrom;

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
