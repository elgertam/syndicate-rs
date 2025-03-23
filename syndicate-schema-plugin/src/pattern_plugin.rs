use preserves_schema::*;
use preserves_schema::compiler::*;
use preserves_schema::compiler::context::ModuleContext;
use preserves_schema::compiler::types::definition_type;
use preserves_schema::compiler::types::Purpose;
use preserves_schema::gen::schema::*;
use preserves_schema::syntax::block::escape_string;
use preserves_schema::syntax::block::constructors::*;

use preserves::IOValue;
use preserves::Map;
use preserves::NoEmbeddedDomainCodec;
use preserves::Value;
use preserves::value_deepcopy_via;

use std::iter::FromIterator;

use crate::pattern::lift_literal;
use crate::schemas::dataspace_patterns as P;

#[derive(Debug)]
pub struct PatternPlugin {
    pub syndicate_crate: String,
}

impl PatternPlugin {
    pub fn new() -> Self {
        PatternPlugin {
            syndicate_crate: "syndicate".to_string(),
        }
    }
}

type WalkState<'a, 'm, 'b> =
    preserves_schema::compiler::cycles::WalkState<&'a ModuleContext<'m, 'b>>;

impl Plugin for PatternPlugin {
    fn generate_definition(
        &self,
        ctxt: &mut ModuleContext,
        definition_name: &str,
        definition: &Definition,
    ) {
        if ctxt.mode == context::ModuleContextMode::TargetGeneric {
            let mut s = WalkState::new(ctxt, ctxt.module_path.clone());
            if let Some(p) = definition.wc(&mut s) {
                let ty = definition_type(&ctxt.module_path,
                                         Purpose::Codegen,
                                         definition_name,
                                         definition);
                let v = crate::language().unparse(&p);
                let v = preserves::TextWriter::encode(&mut NoEmbeddedDomainCodec, &v).unwrap();
                ctxt.define_type(item(seq![
                    "impl",
                    ty.generic_decl(ctxt),
                    " ",
                    names::render_constructor(definition_name),
                    ty.generic_arg(ctxt),
                    " ", codeblock![
                        seq!["#[allow(unused)] pub fn wildcard_dataspace_pattern() ",
                             seq!["-> ", self.syndicate_crate.clone(), "::schemas::dataspace_patterns::Pattern "],
                             codeblock![
                                 seq!["use ", self.syndicate_crate.clone(), "::schemas::dataspace_patterns::*;"],
                                 "use preserves_schema::Codec;",
                                 seq!["let _v = ", self.syndicate_crate.clone(), "::preserves::read_text(",
                                      escape_string(&v),
                                      ", false, ",
                                      "&mut ", self.syndicate_crate.clone(), "::preserves::NoEmbeddedDomainCodec).unwrap();"],
                                 seq![self.syndicate_crate.clone(), "::language().parse(&_v).unwrap()"]]]]]));
            }
        }
    }
}

fn discard() -> P::Pattern {
    P::Pattern::Discard
}

trait WildcardPattern {
    fn wc(&self, s: &mut WalkState) -> Option<P::Pattern>;
}

impl WildcardPattern for Definition {
    fn wc(&self, s: &mut WalkState) -> Option<P::Pattern> {
        match self {
            Definition::Or { .. } => None,
            Definition::And { .. } => None,
            Definition::Pattern(p) => p.wc(s),
        }
    }
}

impl WildcardPattern for Pattern {
    fn wc(&self, s: &mut WalkState) -> Option<P::Pattern> {
        match self {
            Pattern::CompoundPattern(p) => p.wc(s),
            Pattern::SimplePattern(p) => p.wc(s),
        }
    }
}

fn from_io(v: &Value<IOValue>) -> Option<P::_Any> {
    value_deepcopy_via(v.into(), &mut |_| Err::<P::_Any, _>(())).ok()
}

impl WildcardPattern for CompoundPattern {
    fn wc(&self, s: &mut WalkState) -> Option<P::Pattern> {
        match self {
            CompoundPattern::Tuple { patterns } |
            CompoundPattern::TuplePrefix { fixed: patterns, .. }=>
                Some(P::Pattern::Group {
                    type_: P::GroupType::Arr,
                    entries: patterns.iter().enumerate()
                        .map(|(i, p)| Some((P::_Any::new(i), unname(p).wc(s)?)))
                        .collect::<Option<Map<P::_Any, P::Pattern>>>()?,
                }),
            CompoundPattern::Dict { entries } =>
                Some(P::Pattern::Group {
                    type_: P::GroupType::Dict,
                    entries: Map::from_iter(
                        entries.0.iter()
                            .map(|(k, p)| Some((from_io(k)?, unname_simple(p).wc(s)?)))
                            .filter(|e| discard() != e.as_ref().unwrap().1)
                            .collect::<Option<Vec<(P::_Any, P::Pattern)>>>()?
                            .into_iter()),
                }),
            CompoundPattern::Rec { label, fields } => match (unname(label), unname(fields)) {
                (Pattern::SimplePattern(label), Pattern::CompoundPattern(fields)) =>
                    match (label, *fields) {
                        (SimplePattern::Lit { value }, CompoundPattern::Tuple { patterns }) =>
                            Some(P::Pattern::Group{
                                type_: P::GroupType::Rec { label: from_io(&value)? },
                                entries: patterns.iter().enumerate()
                                    .map(|(i, p)| Some((P::_Any::new(i), unname(p).wc(s)?)))
                                    .collect::<Option<Map<P::_Any, P::Pattern>>>()?,
                            }),
                        _ => None,
                    },
                _ => None,
            },
        }
    }
}

impl WildcardPattern for SimplePattern {
    fn wc(&self, s: &mut WalkState) -> Option<P::Pattern> {
        match self {
            SimplePattern::Any |
            SimplePattern::Atom { .. } |
            SimplePattern::Embedded { .. } |
            SimplePattern::Seqof { .. } |
            SimplePattern::Setof { .. } |
            SimplePattern::Dictof { .. } => Some(discard()),
            SimplePattern::Lit { value } => Some(lift_literal(&from_io(value)?)),
            SimplePattern::Ref(r) => s.cycle_check(
                r,
                |ctxt, r| ctxt.bundle.lookup_definition(r).map(|v| v.0),
                |s, d| d.and_then(|d| d.wc(s)).or_else(|| Some(discard())),
                || Some(discard())),
        }
    }
}

fn unname(np: &NamedPattern) -> Pattern {
    match np {
        NamedPattern::Anonymous(p) => (**p).clone(),
        NamedPattern::Named(b) => Pattern::SimplePattern(b.pattern.clone()),
    }
}

fn unname_simple(np: &NamedSimplePattern) -> &SimplePattern {
    match np {
        NamedSimplePattern::Anonymous(p) => p,
        NamedSimplePattern::Named(b) => &b.pattern,
    }
}
