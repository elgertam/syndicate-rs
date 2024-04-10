use preserves_schema::Codec;

use std::io;
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::dataspace::Dataspace;
use syndicate::during;
use syndicate::enclose;
use syndicate::pattern::{lift_literal, drop_literal, pattern_seq_from_dictionary};
use syndicate::schemas::dataspace;
use syndicate::schemas::dataspace_patterns as P;
use syndicate::schemas::sturdy;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::NoEmbeddedDomainCodec;
use syndicate::value::Record;
use syndicate::value::Set;
use syndicate::value::TextWriter;
use syndicate::value::Value;

use crate::language::language;

#[derive(Debug)]
struct PatternInstantiator<'env> {
    env: &'env Env,
    binding_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Env {
    pub path: PathBuf,
    bindings: Map<String, AnyValue>,
}

#[derive(Debug)]
pub struct Parser<'t> {
    tokens: &'t [AnyValue],
    errors: Vec<String>,
}

#[derive(Debug)]
pub enum Parsed<T> {
    Value(T),
    Skip,
    Eof,
}

#[derive(Debug, Clone)]
pub enum Instruction {
    Assert {
        target: String,
        template: AnyValue,
    },
    Message {
        target: String,
        template: AnyValue,
    },
    During {
        target: String,
        pattern_template: AnyValue,
        body: Box<Instruction>,
    },
    OnMessage {
        target: String,
        pattern_template: AnyValue,
        body: Box<Instruction>,
    },
    OnStop {
        body: Box<Instruction>,
    },
    Sequence {
        instructions: Vec<Instruction>,
    },
    Let {
        pattern_template: AnyValue,
        expr: Expr,
    },
    Cond {
        value_var: String,
        pattern_template: AnyValue,
        on_match: Box<Instruction>,
        on_nomatch: Box<Instruction>,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    Template {
        template: AnyValue,
    },
    Dataspace,
    Timestamp,
    Facet,
    Stringify {
        expr: Box<Expr>,
    },
}

#[derive(Debug, Clone)]
enum RewriteTemplate {
    Accept {
        pattern_template: AnyValue,
    },
    Rewrite {
        pattern_template: AnyValue,
        template_template: AnyValue,
    },
}

#[derive(Debug, Clone)]
enum CaveatTemplate {
    Alts {
        alternatives: Vec<RewriteTemplate>,
    },
    Reject {
        pattern_template: AnyValue,
    },
}

#[derive(Debug)]
enum Symbolic {
    Reference(String),
    Binder(String),
    Discard,
    Literal(String),
    Bare(String),
}

struct FacetHandle;

impl<T> Default for Parsed<T> {
    fn default() -> Self {
        Parsed::Skip
    }
}

impl FacetHandle {
    fn new() -> Self {
        FacetHandle
    }
}

impl Entity<AnyValue> for FacetHandle {
    fn message(&mut self, t: &mut Activation, body: AnyValue) -> ActorResult {
        if let Some("stop") = body.value().as_symbol().map(|s| s.as_str()) {
            t.stop();
            return Ok(())
        }
        tracing::warn!(?body, "Unrecognised message sent to FacetHandle");
        return Ok(())
    }
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
        Symbolic::Bare(s.to_owned())
    }
}

fn bad_instruction(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn discard() -> P::Pattern {
    P::Pattern::Discard
}

fn dlit(value: AnyValue) -> P::Pattern {
    lift_literal(&value)
}

fn tlit(value: AnyValue) -> sturdy::Template {
    sturdy::Template::Lit(Box::new(sturdy::Lit { value }))
}

fn parse_rewrite(raw_base_name: &AnyValue, e: &AnyValue) -> io::Result<RewriteTemplate> {
    if let Some(fields) = e.value().as_simple_record("accept", Some(1)) {
        return Ok(RewriteTemplate::Accept {
            pattern_template: fields[0].clone(),
        });
    }

    if let Some(fields) = e.value().as_simple_record("rewrite", Some(2)) {
        return Ok(RewriteTemplate::Rewrite {
            pattern_template: fields[0].clone(),
            template_template: fields[1].clone(),
        });
    }

    Err(bad_instruction(&format!("Bad rewrite in attenuation of {:?}: {:?}", raw_base_name, e)))
}

fn parse_caveat(raw_base_name: &AnyValue, e: &AnyValue) -> io::Result<CaveatTemplate> {
    if let Some(fields) = e.value().as_simple_record("or", Some(1)) {
        let raw_rewrites = match fields[0].value().as_sequence() {
            None => Err(bad_instruction(&format!(
                "Alternatives in <or> in attenuation of {:?} must have sequence of rewrites; got {:?}",
                raw_base_name,
                fields[0])))?,
            Some(vs) => vs,
        };
        let alternatives =
            raw_rewrites.iter().map(|r| parse_rewrite(raw_base_name, r)).collect::<Result<Vec<_>, _>>()?;
        return Ok(CaveatTemplate::Alts{ alternatives });
    }

    if let Some(fields) = e.value().as_simple_record("reject", Some(1)) {
        return Ok(CaveatTemplate::Reject{ pattern_template: fields[0].clone() });
    }

    if let Ok(r) = parse_rewrite(raw_base_name, e) {
        return Ok(CaveatTemplate::Alts { alternatives: vec![r] });
    }

    Err(bad_instruction(&format!("Bad caveat in attenuation of {:?}: {:?}", raw_base_name, e)))
}

fn parse_attenuation(r: &Record<AnyValue>) -> io::Result<Option<(String, Vec<CaveatTemplate>)>> {
    if r.label() != &AnyValue::symbol("*") {
        return Ok(None);
    }

    if r.fields().len() != 2 {
        Err(bad_instruction(&format!(
            "Attenuation requires a reference and a sequence of caveats; got {:?}",
            r)))?;
    }

    let raw_base_name = &r.fields()[0];
    let base_name = match raw_base_name.value().as_symbol().map(|s| analyze(&s)) {
        Some(Symbolic::Reference(s)) => s,
        _ => Err(bad_instruction(&format!(
            "Attenuation must have variable reference as first argument; got {:?}",
            raw_base_name)))?,
    };

    let raw_caveats = match r.fields()[1].value().as_sequence() {
        None => Err(bad_instruction(&format!(
            "Attenuation of {:?} must have sequence of caveats; got {:?}",
            raw_base_name,
            r.fields()[1])))?,
        Some(vs) => vs,
    };

    let caveats = raw_caveats.iter().map(|c| parse_caveat(raw_base_name, c)).collect::<Result<Vec<_>, _>>()?;
    Ok(Some((base_name, caveats)))
}

impl<'env> PatternInstantiator<'env> {
    fn instantiate_pattern(&mut self, template: &AnyValue) -> io::Result<P::Pattern> {
        Ok(match template.value() {
            Value::Boolean(_) |
            Value::Double(_) |
            Value::SignedInteger(_) |
            Value::String(_) |
            Value::ByteString(_) |
            Value::Embedded(_) =>
                dlit(template.clone()),

            Value::Symbol(s) => match analyze(s) {
                Symbolic::Discard => discard(),
                Symbolic::Binder(s) => {
                    self.binding_names.push(s);
                    P::Pattern::Bind { pattern: Box::new(discard()) }
                }
                Symbolic::Reference(s) =>
                    dlit(self.env.lookup(&s, "pattern-template variable")?.clone()),
                Symbolic::Literal(s) | Symbolic::Bare(s) =>
                    dlit(Value::Symbol(s).wrap()),
            },

            Value::Record(r) => match parse_attenuation(r)? {
                Some((base_name, caveats)) =>
                    dlit(self.env.eval_attenuation(base_name, caveats)?),
                None => match self.maybe_binder_with_pattern(r)? {
                    Some(pat) => pat,
                    None => {
                        let label = self.instantiate_pattern(r.label())?;
                        let entries = r.fields().iter().enumerate()
                        .map(|(i, p)| Ok((AnyValue::new(i), self.instantiate_pattern(p)?)))
                            .collect::<io::Result<Map<AnyValue, P::Pattern>>>()?;
                        P::Pattern::Group {
                            type_: Box::new(P::GroupType::Rec {
                                label: drop_literal(&label)
                                    .ok_or(bad_instruction("Record pattern must have literal label"))?,
                            }),
                            entries,
                        }
                    }
                }
            },
            Value::Sequence(v) =>
                P::Pattern::Group {
                    type_: Box::new(P::GroupType::Arr),
                    entries: v.iter().enumerate()
                        .map(|(i, p)| Ok((AnyValue::new(i), self.instantiate_pattern(p)?)))
                        .collect::<io::Result<Map<AnyValue, P::Pattern>>>()?,
                },
            Value::Set(_) =>
                Err(bad_instruction(&format!("Sets not permitted in patterns: {:?}", template)))?,
            Value::Dictionary(v) =>
                P::Pattern::Group {
                    type_: Box::new(P::GroupType::Dict),
                    entries: v.iter()
                        .map(|(a, b)| Ok((a.clone(), self.instantiate_pattern(b)?)))
                        .collect::<io::Result<Map<AnyValue, P::Pattern>>>()?,
                },
        })
    }

    fn maybe_binder_with_pattern(&mut self, r: &Record<AnyValue>) -> io::Result<Option<P::Pattern>> {
        match r.label().value().as_symbol().map(|s| analyze(&s)) {
            Some(Symbolic::Binder(formal)) if r.fields().len() == 1 => {
                let pattern = self.instantiate_pattern(&r.fields()[0])?;
                self.binding_names.push(formal);
                Ok(Some(P::Pattern::Bind { pattern: Box::new(pattern) }))
            },
            _ => Ok(None),
        }
    }
}

impl Env {
    pub fn new(path: PathBuf, bindings: Map<String, AnyValue>) -> Self {
        Env {
            path: path.clone(),
            bindings,
        }
    }

    pub fn clone_with_path(&self, path: PathBuf) -> Self {
        Env {
            path,
            bindings: self.bindings.clone(),
        }
    }

    fn lookup_target(&self, s: &str) -> io::Result<Arc<Cap>> {
        Ok(self.lookup(s, "target variable")?.value().to_embedded()?.clone())
    }

    fn lookup(&self, s: &str, what: &'static str) -> io::Result<AnyValue> {
        if s == "." {
            Ok(AnyValue::new(self.bindings.iter().map(|(k, v)| (AnyValue::symbol(k), v.clone()))
                             .collect::<Map<AnyValue, AnyValue>>()))
        } else {
            Ok(self.bindings.get(s).ok_or_else(
                || bad_instruction(&format!("Undefined {}: {:?}", what, s)))?.clone())
        }
    }

    fn instantiate_pattern(
        &self,
        pattern_template: &AnyValue,
    ) -> io::Result<(Vec<String>, P::Pattern)> {
        let mut inst = PatternInstantiator {
            env: self,
            binding_names: Vec::new(),
        };
        let pattern = inst.instantiate_pattern(pattern_template)?;
        Ok((inst.binding_names, pattern))
    }

    fn instantiate_value(&self, template: &AnyValue) -> io::Result<AnyValue> {
        Ok(match template.value() {
            Value::Boolean(_) |
            Value::Double(_) |
            Value::SignedInteger(_) |
            Value::String(_) |
            Value::ByteString(_) |
            Value::Embedded(_) =>
                template.clone(),

            Value::Symbol(s) => match analyze(s) {
                Symbolic::Binder(_) | Symbolic::Discard =>
                    Err(bad_instruction(&format!(
                        "Invalid use of wildcard in template: {:?}", template)))?,
                Symbolic::Reference(s) =>
                    self.lookup(&s, "template variable")?,
                Symbolic::Literal(s) | Symbolic::Bare(s) =>
                    Value::Symbol(s).wrap(),
            },

            Value::Record(r) => match parse_attenuation(r)? {
                Some((base_name, caveats)) =>
                    self.eval_attenuation(base_name, caveats)?,
                None =>
                    Value::Record(Record(r.fields_vec().iter().map(|a| self.instantiate_value(a))
                                         .collect::<Result<Vec<_>, _>>()?)).wrap(),
            },
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

    pub fn safe_eval(&mut self, t: &mut Activation, i: &Instruction) -> bool {
        match self.eval(t, i) {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(path = ?self.path, ?error);
                t.stop();
                false
            }
        }
    }

    pub fn extend(&mut self, binding_names: &Vec<String>, captures: Vec<AnyValue>) {
        for (k, v) in binding_names.iter().zip(captures) {
            self.bindings.insert(k.clone(), v);
        }
    }

    fn eval_attenuation(
        &self,
        base_name: String,
        caveats: Vec<CaveatTemplate>,
    ) -> io::Result<AnyValue> {
        let base_value = self.lookup(&base_name, "attenuation-base variable")?;
        match base_value.value().as_embedded() {
            None => Err(bad_instruction(&format!(
                "Value to be attenuated is {:?} but must be capability",
                base_value))),
            Some(base_cap) => {
                match base_cap.attenuate(&caveats.iter().map(|c| self.instantiate_caveat(c)).collect::<Result<Vec<_>, _>>()?) {
                    Ok(derived_cap) => Ok(AnyValue::domain(derived_cap)),
                    Err(caveat_error) =>
                        Err(bad_instruction(&format!("Attenuation of {:?} failed: {:?}",
                                                     base_value,
                                                     caveat_error))),
                }
            }
        }
    }

    fn bind_and_run(
        &self,
        t: &mut Activation,
        binding_names: &Vec<String>,
        captures: AnyValue,
        body: &Instruction,
    ) -> ActorResult {
        if let Some(captures) = captures.value_owned().into_sequence() {
            let mut env = self.clone();
            env.extend(binding_names, captures);
            env.safe_eval(t, body);
        }
        Ok(())
    }

    pub fn eval(&mut self, t: &mut Activation, i: &Instruction) -> io::Result<()> {
        match i {
            Instruction::Assert { target, template } => {
                self.lookup_target(target)?.assert(t, &(), &self.instantiate_value(template)?);
            }
            Instruction::Message { target, template } => {
                self.lookup_target(target)?.message(t, &(), &self.instantiate_value(template)?);
            }
            Instruction::During { target, pattern_template, body } => {
                let (binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                let observer = during::entity(self.clone())
                    .on_asserted_facet(enclose!((binding_names, body) move |env, t, cs: AnyValue| {
                        env.bind_and_run(t, &binding_names, cs, &*body) }))
                    .create_cap(t);
                self.lookup_target(target)?.assert(t, language(), &dataspace::Observe {
                    pattern,
                    observer,
                });
            }
            Instruction::OnMessage { target, pattern_template, body } => {
                let (binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                let observer = during::entity(self.clone())
                    .on_message(enclose!((binding_names, body) move |env, t, cs: AnyValue| {
                        t.facet(|t| env.bind_and_run(t, &binding_names, cs, &*body))?;
                        Ok(())
                    }))
                    .create_cap(t);
                self.lookup_target(target)?.assert(t, language(), &dataspace::Observe {
                    pattern,
                    observer,
                });
            }
            Instruction::OnStop { body } => {
                let mut env = self.clone();
                t.on_stop(enclose!((body) move |t| Ok(env.eval(t, &*body)?)));
            }
            Instruction::Sequence { instructions } => {
                for i in instructions {
                    self.eval(t, i)?;
                }
            }
            Instruction::Let { pattern_template, expr } => {
                let (binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                let value = self.eval_expr(t, expr)?;
                match pattern.match_value(&value) {
                    None => Err(bad_instruction(
                        &format!("Could not match pattern {:?} with value {:?}",
                                 pattern_template,
                                 value)))?,
                    Some(captures) => {
                        self.extend(&binding_names, captures);
                    }
                }
            }
            Instruction::Cond { value_var, pattern_template, on_match, on_nomatch } => {
                let (binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                let value = self.lookup(value_var, "value in conditional expression")?;
                match pattern.match_value(&value) {
                    None => self.eval(t, on_nomatch)?,
                    Some(captures) => {
                        self.extend(&binding_names, captures);
                        self.eval(t, on_match)?
                    }
                }
            }
        }
        Ok(())
    }

    pub fn eval_expr(&self, t: &mut Activation, e: &Expr) -> io::Result<AnyValue> {
        match e {
            Expr::Template { template } => self.instantiate_value(template),
            Expr::Dataspace => Ok(AnyValue::domain(Cap::new(&t.create(Dataspace::new(None))))),
            Expr::Timestamp => Ok(AnyValue::new(chrono::Utc::now().to_rfc3339())),
            Expr::Facet => Ok(AnyValue::domain(Cap::new(&t.create(FacetHandle::new())))),
            Expr::Stringify { expr } => {
                let v = self.eval_expr(t, expr)?;
                let s = TextWriter::encode(&mut NoEmbeddedDomainCodec, &v)?;
                Ok(AnyValue::new(s))
            }
        }
    }

    fn instantiate_rewrite(
        &self,
        rw: &RewriteTemplate,
    ) -> io::Result<sturdy::Rewrite> {
        match rw {
            RewriteTemplate::Accept { pattern_template } => {
                let (_binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                Ok(sturdy::Rewrite {
                    pattern: embed_pattern(&P::Pattern::Bind { pattern: Box::new(pattern) }),
                    template: sturdy::Template::TRef(Box::new(sturdy::TRef { binding: 0.into() })),
                })
            }
            RewriteTemplate::Rewrite { pattern_template, template_template } => {
                let (binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                Ok(sturdy::Rewrite {
                    pattern: embed_pattern(&pattern),
                    template: self.instantiate_template(&binding_names, template_template)?,
                })
            }
        }
    }

    fn instantiate_caveat(
        &self,
        c: &CaveatTemplate,
    ) -> io::Result<sturdy::Caveat> {
        match c {
            CaveatTemplate::Alts { alternatives } => {
                let mut rewrites =
                    alternatives.iter().map(|r| self.instantiate_rewrite(r)).collect::<Result<Vec<_>, _>>()?;
                if rewrites.len() == 1 {
                    Ok(sturdy::Caveat::Rewrite(Box::new(rewrites.pop().unwrap())))
                } else {
                    Ok(sturdy::Caveat::Alts(Box::new(sturdy::Alts {
                        alternatives: rewrites,
                    })))
                }
            }
            CaveatTemplate::Reject { pattern_template } => {
                Ok(sturdy::Caveat::Reject(Box::new(
                    sturdy::Reject {
                        pattern: embed_pattern(&self.instantiate_pattern(pattern_template)?.1),
                    })))
            }
        }
    }

    fn instantiate_template(
        &self,
        binding_names: &Vec<String>,
        template: &AnyValue,
    ) -> io::Result<sturdy::Template> {
        let find_bound = |s: &str| {
            binding_names.iter().enumerate().find(|(_i, n)| *n == s).map(|(i, _n)| i)
        };

        Ok(match template.value() {
            Value::Boolean(_) |
            Value::Double(_) |
            Value::SignedInteger(_) |
            Value::String(_) |
            Value::ByteString(_) |
            Value::Embedded(_) =>
                tlit(template.clone()),

            Value::Symbol(s) => match analyze(s) {
                Symbolic::Binder(_) | Symbolic::Discard =>
                    Err(bad_instruction(&format!(
                        "Invalid use of wildcard in template: {:?}", template)))?,
                Symbolic::Reference(s) =>
                    match find_bound(&s) {
                        Some(i) =>
                            sturdy::Template::TRef(Box::new(sturdy::TRef { binding: i.into() })),
                        None =>
                            tlit(self.lookup(&s, "attenuation-template variable")?),
                    },
                Symbolic::Literal(s) | Symbolic::Bare(s) =>
                    tlit(Value::Symbol(s).wrap()),
            },

            Value::Record(r) => match parse_attenuation(r)? {
                Some((base_name, caveats)) =>
                    match find_bound(&base_name) {
                        Some(i) =>
                            sturdy::Template::TAttenuate(Box::new(sturdy::TAttenuate {
                                template: sturdy::Template::TRef(Box::new(sturdy::TRef {
                                    binding: i.into(),
                                })),
                                attenuation: caveats.iter()
                                    .map(|c| self.instantiate_caveat(c))
                                    .collect::<Result<Vec<_>, _>>()?,
                            })),
                        None =>
                            tlit(self.eval_attenuation(base_name, caveats)?),
                    },
                None => {
                    // TODO: properly consolidate constant templates into literals.
                    match self.instantiate_template(binding_names, r.label())? {
                        sturdy::Template::Lit(b) =>
                            sturdy::Template::TCompound(Box::new(sturdy::TCompound::Rec {
                                label: b.value,
                                fields: r.fields().iter()
                                    .map(|t| self.instantiate_template(binding_names, t))
                                    .collect::<io::Result<Vec<sturdy::Template>>>()?,
                            })),
                        _ => Err(bad_instruction("Record template must have literal label"))?,
                    }
                }
            },
            Value::Sequence(v) =>
                sturdy::Template::TCompound(Box::new(sturdy::TCompound::Arr {
                    items: v.iter()
                        .map(|p| self.instantiate_template(binding_names, p))
                        .collect::<io::Result<Vec<sturdy::Template>>>()?,
                })),
            Value::Set(_) =>
                Err(bad_instruction(&format!("Sets not permitted in templates: {:?}", template)))?,
            Value::Dictionary(v) =>
                sturdy::Template::TCompound(Box::new(sturdy::TCompound::Dict {
                    entries: v.iter()
                        .map(|(a, b)| Ok((a.clone(), self.instantiate_template(binding_names, b)?)))
                        .collect::<io::Result<Map<_, sturdy::Template>>>()?,
                })),
        })
    }
}

fn embed_pattern(p: &P::Pattern) -> sturdy::Pattern {
    match p {
        P::Pattern::Discard => sturdy::Pattern::PDiscard(Box::new(sturdy::PDiscard)),
        P::Pattern::Bind { pattern } => sturdy::Pattern::PBind(Box::new(sturdy::PBind {
            pattern: embed_pattern(&**pattern),
        })),
        P::Pattern::Lit { value } => sturdy::Pattern::Lit(Box::new(sturdy::Lit {
            value: language().unparse(&**value),
        })),
        P::Pattern::Group { type_, entries } => sturdy::Pattern::PCompound(Box::new(match &**type_ {
            P::GroupType::Rec { label } =>
                sturdy::PCompound::Rec {
                    label: label.clone(),
                    fields: pattern_seq_from_dictionary(entries).expect("correct field entries")
                        .into_iter().map(embed_pattern).collect(),
                },
            P::GroupType::Arr =>
                sturdy::PCompound::Arr {
                    items: pattern_seq_from_dictionary(entries).expect("correct element entries")
                        .into_iter().map(embed_pattern).collect(),
                },
            P::GroupType::Dict =>
                sturdy::PCompound::Dict {
                    entries: entries.iter().map(|(k, v)| (k.clone(), embed_pattern(v))).collect(),
                },
        })),
    }
}

impl<'t> Parser<'t> {
    pub fn new(tokens: &'t [AnyValue]) -> Self {
        Parser {
            tokens,
            errors: Vec::new(),
        }
    }

    fn peek(&mut self) -> &'t Value<AnyValue> {
        self.tokens[0].value()
    }

    fn shift(&mut self) -> AnyValue {
        let v = self.tokens[0].clone();
        self.drop();
        v
    }

    fn drop(&mut self) {
        self.tokens = &self.tokens[1..];
    }

    fn len(&self) -> usize {
        self.tokens.len()
    }

    fn ateof(&self) -> bool {
        self.len() == 0
    }

    fn error<'a, T: Default, E: Into<Cow<'a, str>>>(&mut self, message: E) -> T {
        self.errors.push(message.into().into_owned());
        T::default()
    }

    pub fn parse(&mut self, target: &str, outer_target: &str) -> Parsed<Instruction> {
        if self.ateof() {
            return Parsed::Eof;
        }

        if self.peek().is_record() || self.peek().is_dictionary() {
            return Parsed::Value(Instruction::Assert {
                target: target.to_owned(),
                template: self.shift(),
            });
        }

        if let Some(tokens) = self.peek().as_sequence() {
            self.drop();
            let mut inner_parser = Parser::new(tokens);
            let instructions = inner_parser.parse_all(target, outer_target);
            self.errors.extend(inner_parser.errors);
            return Parsed::Value(Instruction::Sequence { instructions });
        }

        if let Some(s) = self.peek().as_symbol() {
            match analyze(s) {
                Symbolic::Binder(s) => {
                    self.drop();

                    let ctor = match s.as_ref() {
                        "" => |target, pattern_template, body| { // "?"
                            Instruction::During { target, pattern_template, body } },
                        "?" => |target, pattern_template, body| { // "??"
                            Instruction::OnMessage { target, pattern_template, body } },
                        "-" => match self.parse(target, outer_target) { // "?-"
                            Parsed::Value(i) => return Parsed::Value(Instruction::OnStop {
                                body: Box::new(i),
                            }),
                            other => return other,
                        },
                        _ => return self.error(format!(
                            "Invalid use of pattern binder in target: ?{}", s)),
                    };

                    if self.ateof() {
                        return self.error("Missing pattern and instruction in react");
                    }
                    let pattern_template = self.shift();

                    return match self.parse(target, outer_target) {
                        Parsed::Eof =>
                            self.error(format!(
                                "Missing instruction in react with pattern {:?}",
                                pattern_template)),
                        Parsed::Skip =>
                            Parsed::Skip,
                        Parsed::Value(body) =>
                            Parsed::Value(ctor(target.to_owned(),
                                               pattern_template,
                                               Box::new(body))),
                    };
                }
                Symbolic::Discard => {
                    self.drop();
                    let m = format!("Invalid use of discard in target: {:?}", self.peek());
                    return self.error(m);
                },
                Symbolic::Reference(s) => {
                    self.drop();
                    if self.ateof() {
                        let m = format!("Missing instruction after retarget: {:?}", self.peek());
                        return self.error(m);
                    }
                    return self.parse(&s, target);
                }
                Symbolic::Bare(s) => {
                    if s == "let" {
                        self.drop();
                        if self.len() >= 2 && self.tokens[1].value().as_symbol().map(String::as_str) == Some("=")
                        {
                            let pattern_template = self.shift();
                            self.drop();
                            return match self.parse_expr() {
                                Some(expr) =>
                                    Parsed::Value(Instruction::Let { pattern_template, expr }),
                                None => Parsed::Skip,
                            };
                        } else {
                            return self.error("Invalid let statement");
                        }
                    } else if s == "!" {
                        self.drop();
                        if self.ateof() {
                            return self.error("Missing payload after '!'");
                        }
                        return Parsed::Value(Instruction::Message {
                            target: target.to_owned(),
                            template: self.shift(),
                        });
                    } else if s == "+=" {
                        self.drop();
                        if self.ateof() {
                            return self.error("Missing payload after '+='");
                        }
                        return Parsed::Value(Instruction::Assert {
                            target: target.to_owned(),
                            template: self.shift(),
                        });
                    } else {
                        /* fall through */
                    }
                }
                Symbolic::Literal(s) => {
                    if s == "~" { // "=~"
                        self.drop();
                        if self.ateof() {
                            return self.error("Missing pattern, true-instruction and false-continuation in match");
                        }
                        let match_template = self.shift();
                        return match self.parse(outer_target, outer_target) {
                            Parsed::Eof =>
                                self.error(format!(
                                    "Missing true-instruction in conditional with pattern {:?}",
                                    match_template)),
                            Parsed::Skip =>
                                Parsed::Skip,
                            Parsed::Value(true_instruction) => {
                                let false_instructions = self.parse_all(outer_target, outer_target);
                                Parsed::Value(Instruction::Cond {
                                    value_var: target.to_owned(),
                                    pattern_template: match_template,
                                    on_match: Box::new(true_instruction),
                                    on_nomatch: Box::new(Instruction::Sequence {
                                        instructions: false_instructions,
                                    }),
                                })
                            }
                        };
                    } else {
                        /* fall through */
                    }
                }
            }
        }

        {
            let m = format!("Invalid token: {:?}", self.shift());
            return self.error(m);
        }
    }

    pub fn parse_all(&mut self, target: &str, outer_target: &str) -> Vec<Instruction> {
        let mut instructions = Vec::new();
        loop {
            match self.parse(target, outer_target) {
                Parsed::Value(i) => instructions.push(i),
                Parsed::Skip => (),
                Parsed::Eof => break,
            }
        }
        instructions
    }

    pub fn parse_top(&mut self, target: &str) -> Result<Option<Instruction>, Vec<String>> {
        let instructions = self.parse_all(target, target);
        if self.errors.is_empty() {
            match instructions.len() {
                0 => Ok(None),
                _ => Ok(Some(Instruction::Sequence { instructions })),
            }
        } else {
            Err(std::mem::take(&mut self.errors))
        }
    }

    pub fn parse_expr(&mut self) -> Option<Expr> {
        if self.ateof() {
            return None;
        }

        if self.peek() == &Value::symbol("dataspace") {
            self.drop();
            return Some(Expr::Dataspace);
        }

        if self.peek() == &Value::symbol("timestamp") {
            self.drop();
            return Some(Expr::Timestamp);
        }

        if self.peek() == &Value::symbol("facet") {
            self.drop();
            return Some(Expr::Facet);
        }

        if self.peek() == &Value::symbol("stringify") {
            self.drop();
            return Some(Expr::Stringify { expr: Box::new(self.parse_expr()?) });
        }

        return Some(Expr::Template{ template: self.shift() });
    }
}
