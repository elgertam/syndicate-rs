use preserves_schema::Codec;

use std::io;
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::dataspace::Dataspace;
use syndicate::during;
use syndicate::enclose;
use syndicate::pattern::{lift_literal, drop_literal};
use syndicate::schemas::dataspace;
use syndicate::schemas::dataspace_patterns as P;
use syndicate::schemas::sturdy;
use syndicate::value::Map;
use syndicate::value::NestedValue;
use syndicate::value::Record;
use syndicate::value::Set;
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
}

#[derive(Debug, Clone)]
pub enum Expr {
    Template {
        template: AnyValue,
    },
    Dataspace,
    Timestamp,
    Facet,
}

#[derive(Debug, Clone)]
enum RewriteTemplate {
    Filter {
        pattern_template: AnyValue,
    },
    Rewrite {
        pattern_template: AnyValue,
        template_template: AnyValue,
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
    P::Pattern::DDiscard(Box::new(P::DDiscard))
}

fn dlit(value: AnyValue) -> P::Pattern {
    lift_literal(&value)
}

fn tlit(value: AnyValue) -> sturdy::Template {
    sturdy::Template::Lit(Box::new(sturdy::Lit { value }))
}

fn parse_attenuation(r: &Record<AnyValue>) -> io::Result<Option<(String, Vec<RewriteTemplate>)>> {
    if r.label() != &AnyValue::symbol("*") {
        return Ok(None);
    }

    if r.fields().len() != 2 {
        Err(bad_instruction(&format!(
            "Attenuation requires a reference and a sequence of rewrites; got {:?}",
            r)))?;
    }

    let base_name = match r.fields()[0].value().as_symbol().map(|s| analyze(&s)) {
        Some(Symbolic::Reference(s)) => s,
        _ => Err(bad_instruction(&format!(
            "Attenuation must have variable reference as first argument; got {:?}",
            r.fields()[0])))?,
    };

    let raw_alternatives = match r.fields()[1].value().as_sequence() {
        None => Err(bad_instruction(&format!(
            "Attenuation of {:?} must have sequence of rewrites; got {:?}",
            r.fields()[0],
            r.fields()[1])))?,
        Some(vs) => vs,
    };

    let mut alternatives = Vec::new();

    for e in raw_alternatives.iter() {
        match e.value().as_simple_record("filter", Some(1)) {
            Some(fields) =>
                alternatives.push(RewriteTemplate::Filter {
                    pattern_template: fields[0].clone()
                }),
            None => match e.value().as_simple_record("rewrite", Some(2)) {
                Some(fields) =>
                    alternatives.push(RewriteTemplate::Rewrite {
                        pattern_template: fields[0].clone(),
                        template_template: fields[1].clone(),
                    }),
                None => Err(bad_instruction(&format!(
                    "Bad rewrite in attenuation of {:?}: {:?}",
                    r.fields()[0],
                    e)))?,
            }
        }
    }

    Ok(Some((base_name, alternatives)))
}

impl<'env> PatternInstantiator<'env> {
    fn instantiate_pattern(&mut self, template: &AnyValue) -> io::Result<P::Pattern> {
        Ok(match template.value() {
            Value::Boolean(_) |
            Value::Float(_) |
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
                    P::Pattern::DBind(Box::new(P::DBind { pattern: discard() }))
                }
                Symbolic::Reference(s) =>
                    dlit(self.env.lookup(&s, "pattern-template variable")?.clone()),
                Symbolic::Literal(s) | Symbolic::Bare(s) =>
                    dlit(Value::Symbol(s).wrap()),
            },

            Value::Record(r) => match parse_attenuation(r)? {
                Some((base_name, alternatives)) =>
                    dlit(self.env.eval_attenuation(base_name, alternatives)?),
                None =>  {
                    let label = self.instantiate_pattern(r.label())?;
                    let fields = r.fields().iter().map(|p| self.instantiate_pattern(p))
                        .collect::<io::Result<Vec<P::Pattern>>>()?;
                    P::Pattern::DCompound(Box::new(P::DCompound::Rec {
                        label: drop_literal(&label)
                            .ok_or(bad_instruction("Record pattern must have literal label"))?,
                        fields,
                    }))
                }
            },
            Value::Sequence(v) =>
                P::Pattern::DCompound(Box::new(P::DCompound::Arr {
                    items: v.iter()
                        .map(|p| self.instantiate_pattern(p))
                        .collect::<io::Result<Vec<P::Pattern>>>()?,
                })),
            Value::Set(_) =>
                Err(bad_instruction(&format!("Sets not permitted in patterns: {:?}", template)))?,
            Value::Dictionary(v) =>
                P::Pattern::DCompound(Box::new(P::DCompound::Dict {
                    entries: v.iter()
                        .map(|(a, b)| Ok((a.clone(), self.instantiate_pattern(b)?)))
                        .collect::<io::Result<Map<AnyValue, P::Pattern>>>()?,
                })),
        })
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
            Value::Float(_) |
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
                Some((base_name, alternatives)) =>
                    self.eval_attenuation(base_name, alternatives)?,
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
        alternatives: Vec<RewriteTemplate>,
    ) -> io::Result<AnyValue> {
        let base_value = self.lookup(&base_name, "attenuation-base variable")?;
        match base_value.value().as_embedded() {
            None => Err(bad_instruction(&format!(
                "Value to be attenuated is {:?} but must be capability",
                base_value))),
            Some(base_cap) => {
                match base_cap.attenuate(&sturdy::Attenuation(vec![
                    self.instantiate_caveat(&alternatives)?]))
                {
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
        }
        Ok(())
    }

    pub fn eval_expr(&self, t: &mut Activation, e: &Expr) -> io::Result<AnyValue> {
        match e {
            Expr::Template { template } => self.instantiate_value(template),
            Expr::Dataspace => Ok(AnyValue::domain(Cap::new(&t.create(Dataspace::new())))),
            Expr::Timestamp => Ok(AnyValue::new(chrono::Utc::now().to_rfc3339())),
            Expr::Facet => Ok(AnyValue::domain(Cap::new(&t.create(FacetHandle::new())))),
        }
    }

    fn instantiate_caveat(
        &self,
        alternatives: &Vec<RewriteTemplate>,
    ) -> io::Result<sturdy::Caveat> {
        let mut rewrites = Vec::new();
        for rw in alternatives {
            match rw {
                RewriteTemplate::Filter { pattern_template } => {
                    let (_binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                    rewrites.push(sturdy::Rewrite {
                        pattern: embed_pattern(&P::Pattern::DBind(Box::new(P::DBind { pattern }))),
                        template: sturdy::Template::TRef(Box::new(sturdy::TRef { binding: 0.into() })),
                    })
                }
                RewriteTemplate::Rewrite { pattern_template, template_template } => {
                    let (binding_names, pattern) = self.instantiate_pattern(pattern_template)?;
                    rewrites.push(sturdy::Rewrite {
                        pattern: embed_pattern(&pattern),
                        template: self.instantiate_template(&binding_names, template_template)?,
                    })
                }
            }
        }
        if rewrites.len() == 1 {
            Ok(sturdy::Caveat::Rewrite(Box::new(rewrites.pop().unwrap())))
        } else {
            Ok(sturdy::Caveat::Alts(Box::new(sturdy::Alts {
                alternatives: rewrites,
            })))
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
            Value::Float(_) |
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
                Some((base_name, alternatives)) =>
                    match find_bound(&base_name) {
                        Some(i) =>
                            sturdy::Template::TAttenuate(Box::new(sturdy::TAttenuate {
                                template: sturdy::Template::TRef(Box::new(sturdy::TRef {
                                    binding: i.into(),
                                })),
                                attenuation: sturdy::Attenuation(vec![
                                    self.instantiate_caveat(&alternatives)?]),
                            })),
                        None =>
                            tlit(self.eval_attenuation(base_name, alternatives)?),
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
        P::Pattern::DDiscard(_) => sturdy::Pattern::PDiscard(Box::new(sturdy::PDiscard)),
        P::Pattern::DBind(b) => sturdy::Pattern::PBind(Box::new(sturdy::PBind {
            pattern: embed_pattern(&b.pattern),
        })),
        P::Pattern::DLit(b) => sturdy::Pattern::Lit(Box::new(sturdy::Lit {
            value: language().unparse(&b.value),
        })),
        P::Pattern::DCompound(b) => sturdy::Pattern::PCompound(Box::new(match &**b {
            P::DCompound::Rec { label, fields } =>
                sturdy::PCompound::Rec {
                    label: label.clone(),
                    fields: fields.iter().map(embed_pattern).collect(),
                },
            P::DCompound::Arr { items } =>
                sturdy::PCompound::Arr {
                    items: items.iter().map(embed_pattern).collect(),
                },
            P::DCompound::Dict { entries } =>
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

    pub fn parse(&mut self, target: &str) -> Parsed<Instruction> {
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
            return Parsed::Value(Instruction::Sequence {
                instructions: Parser::new(tokens).parse_all(target),
            });
        }

        if let Some(s) = self.peek().as_symbol() {
            match analyze(s) {
                Symbolic::Binder(s) => {
                    self.drop();

                    let ctor = match s.as_ref() {
                        "" => |target, pattern_template, body| {
                            Instruction::During { target, pattern_template, body } },
                        "?" => |target, pattern_template, body| {
                            Instruction::OnMessage { target, pattern_template, body } },
                        "-" => match self.parse(target) {
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

                    return match self.parse(target) {
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
                    return self.parse(&s);
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
                    } else {
                        /* fall through */
                    }
                }
                Symbolic::Literal(_) => {
                    /* fall through */
                }
            }
        }

        {
            let m = format!("Invalid token: {:?}", self.shift());
            return self.error(m);
        }
    }

    pub fn parse_all(&mut self, target: &str) -> Vec<Instruction> {
        let mut instructions = Vec::new();
        loop {
            match self.parse(target) {
                Parsed::Value(i) => instructions.push(i),
                Parsed::Skip => (),
                Parsed::Eof => break,
            }
        }
        instructions
    }

    pub fn parse_top(&mut self, target: &str) -> Result<Option<Instruction>, Vec<String>> {
        let instructions = self.parse_all(target);
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

        return Some(Expr::Template{ template: self.shift() });
    }
}
