use preserves_schema::compiler::*;

mod pattern_plugin {
    use preserves_schema::*;
    use preserves_schema::compiler::*;
    use preserves_schema::compiler::context::ModuleContext;
    use preserves_schema::gen::schema::*;
    use preserves_schema::syntax::block::escape_string;
    use preserves_schema::syntax::block::constructors::*;

    use std::iter::FromIterator;

    use syndicate::pattern::lift_literal;
    use syndicate::schemas::dataspace_patterns as P;
    use syndicate::value::IOValue;
    use syndicate::value::Map;
    use syndicate::value::NestedValue;

    #[derive(Debug)]
    pub struct PatternPlugin;

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
                    let v = syndicate::language().unparse(&p);
                    let v = preserves_schema::support::preserves::value::TextWriter::encode(
                        &mut preserves_schema::support::preserves::value::NoEmbeddedDomainCodec,
                        &v).unwrap();
                    ctxt.define_type(item(seq![
                        "impl ", definition_name.to_owned(), " ", codeblock![
                            seq!["#[allow(unused)] pub fn wildcard_dataspace_pattern() ",
                                 "-> syndicate::schemas::dataspace_patterns::Pattern ",
                                 codeblock![
                                     "use syndicate::schemas::dataspace_patterns::*;",
                                     "use preserves_schema::Codec;",
                                     seq!["let _v = syndicate::value::text::from_str(",
                                          escape_string(&v),
                                          ", syndicate::value::ViaCodec::new(syndicate::value::NoEmbeddedDomainCodec)).unwrap();"],
                                     "syndicate::language().parse(&_v).unwrap()"]]]]));
                }
            }
        }
    }

    fn discard() -> P::Pattern {
        P::Pattern::DDiscard(Box::new(P::DDiscard))
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

    fn from_io(v: &IOValue) -> Option<P::_Any> {
        Some(v.value().copy_via(&mut |_| Err(())).ok()?.wrap())
    }

    impl WildcardPattern for CompoundPattern {
        fn wc(&self, s: &mut WalkState) -> Option<P::Pattern> {
            match self {
                CompoundPattern::Tuple { patterns } =>
                    Some(P::Pattern::DCompound(Box::new(P::DCompound::Arr {
                        items: patterns.iter()
                            .map(|p| unname(p).wc(s))
                            .collect::<Option<Vec<P::Pattern>>>()?,
                    }))),
                CompoundPattern::TuplePrefix { .. } =>
                    Some(discard()),
                CompoundPattern::Dict { entries } =>
                    Some(P::Pattern::DCompound(Box::new(P::DCompound::Dict {
                        entries: Map::from_iter(
                            entries.0.iter()
                                .map(|(k, p)| Some((from_io(k)?, unname_simple(p).wc(s)?)))
                                .filter(|e| discard() != e.as_ref().unwrap().1)
                                .collect::<Option<Vec<(P::_Any, P::Pattern)>>>()?
                                .into_iter()),
                    }))),
                CompoundPattern::Rec { label, fields } => match (unname(label), unname(fields)) {
                    (Pattern::SimplePattern(label), Pattern::CompoundPattern(fields)) =>
                        match (*label, *fields) {
                            (SimplePattern::Lit { value }, CompoundPattern::Tuple { patterns }) =>
                                Some(P::Pattern::DCompound(Box::new(P::DCompound::Rec {
                                    label: from_io(&value)?,
                                    fields: patterns.iter()
                                        .map(|p| unname(p).wc(s))
                                        .collect::<Option<Vec<P::Pattern>>>()?,
                                }))),
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
            NamedPattern::Named(b) => Pattern::SimplePattern(Box::new(b.pattern.clone())),
        }
    }

    fn unname_simple(np: &NamedSimplePattern) -> &SimplePattern {
        match np {
            NamedSimplePattern::Anonymous(p) => p,
            NamedSimplePattern::Named(b) => &b.pattern,
        }
    }
}

fn main() -> std::io::Result<()> {
    let buildroot = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new(gen_dir, "crate::schemas".to_owned());
    c.plugins.push(Box::new(pattern_plugin::PatternPlugin));
    c.add_external_module(ExternalModule::new(vec!["EntityRef".to_owned()], "syndicate::actor"));
    c.add_external_module(
        ExternalModule::new(vec!["TransportAddress".to_owned()],
                            "syndicate::schemas::transport_address")
            .set_fallback_language_types(
                |v| vec![format!("syndicate::schemas::Language<{}>", v)].into_iter().collect()));
    c.add_external_module(
        ExternalModule::new(vec!["gatekeeper".to_owned()], "syndicate::schemas::gatekeeper")
            .set_fallback_language_types(
                |v| vec![format!("syndicate::schemas::Language<{}>", v)].into_iter().collect())
    );
    c.add_external_module(
        ExternalModule::new(vec!["noise".to_owned()], "syndicate::schemas::noise")
            .set_fallback_language_types(
                |v| vec![format!("syndicate::schemas::Language<{}>", v)].into_iter().collect())
    );

    let inputs = expand_inputs(&vec!["protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs, &vec![])?;
    c.load_xref_bin("syndicate", syndicate::schemas::_bundle())?;
    compile(&c)
}
