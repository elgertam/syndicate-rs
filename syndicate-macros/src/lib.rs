#![feature(proc_macro_span)]

use syndicate::value::IOValue;
use syndicate::value::NestedValue;
use syndicate::value::Value;
use syndicate::value::text::iovalue_from_str;

use proc_macro2::Span;
use proc_macro2::TokenStream;

use quote::quote;

use std::convert::TryFrom;

use syn::parse_macro_input;
use syn::ExprLit;
use syn::Ident;
use syn::Lit;
use syn::LitByteStr;

mod dur;
mod pat;
mod stx;
mod val;

use pat::lit;

enum SymbolVariant<'a> {
    Normal(&'a str),
    Binder(&'a str),
    Substitution(&'a str),
    Discard,
}

fn compile_sequence_members(vs: &[IOValue]) -> Vec<TokenStream> {
    vs.iter().enumerate().map(|(i, f)| {
        let p = compile_pattern(f);
        quote!((#i .into(), #p))
    }).collect::<Vec<_>>()
}

fn analyze_symbol(s: &str, allow_binding_and_substitution: bool) -> SymbolVariant {
    if !allow_binding_and_substitution {
        SymbolVariant::Normal(s)
    } else if s.starts_with("$") {
        SymbolVariant::Binder(&s[1..])
    } else if s.starts_with("=") {
        SymbolVariant::Substitution(&s[1..])
    } else if s == "_" {
        SymbolVariant::Discard
    } else {
        SymbolVariant::Normal(s)
    }
}

struct ValueCompiler {
    allow_binding_and_substitution: bool,
}

impl ValueCompiler {
    fn for_patterns() -> Self {
        ValueCompiler {
            allow_binding_and_substitution: false,
        }
    }

    fn for_templates() -> Self {
        ValueCompiler {
            allow_binding_and_substitution: true,
        }
    }

    fn compile(&self, v: &IOValue) -> TokenStream {
        #[allow(non_snake_case)]
        let V_: TokenStream = quote!(syndicate::value);

        let walk = |w| self.compile(w);

        match v.value() {
            Value::Boolean(b) =>
                quote!(#V_::Value::from(#b).wrap()),
            Value::Float(f) => {
                let f = f.0;
                quote!(#V_::Value::from(#f).wrap())
            }
            Value::Double(d) => {
                let d = d.0;
                quote!(#V_::Value::from(#d).wrap())
            }
            Value::SignedInteger(i) => {
                let i = i128::try_from(i).expect("Literal integer out-of-range");
                quote!(#V_::Value::from(#i).wrap())
            }
            Value::String(s) =>
                quote!(#V_::Value::from(#s).wrap()),
            Value::ByteString(bs) => {
                let bs = LitByteStr::new(bs, Span::call_site());
                quote!(#V_::Value::from(#bs).wrap())
            }
            Value::Symbol(s) => match analyze_symbol(&s, self.allow_binding_and_substitution) {
                SymbolVariant::Normal(s) =>
                    quote!(#V_::Value::symbol(#s).wrap()),
                SymbolVariant::Binder(_) |
                SymbolVariant::Discard =>
                    panic!("Binding/Discard not supported here"),
                SymbolVariant::Substitution(s) => {
                    let i = Ident::new(s, Span::call_site());
                    quote!(#i)
                }
            }
            Value::Record(r) => {
                let arity = r.arity();
                let label = walk(r.label());
                let fs: Vec<_> = r.fields().iter().map(walk).collect();
                quote!({
                    let mut ___r = #V_::Value::record(#label, #arity);
                    #(___r.fields_vec_mut().push(#fs);)*
                    ___r.finish().wrap()
                })
            }
            Value::Sequence(vs) => {
                let vs: Vec<_> = vs.iter().map(walk).collect();
                quote!(#V_::Value::from(vec![#(#vs),*]).wrap())
            }
            Value::Set(vs) => {
                let vs: Vec<_> = vs.iter().map(walk).collect();
                quote!({
                    let mut ___s = #V_::Set::new();
                    #(___s.insert(#vs);)*
                    #V_::Value::from(___s).wrap()
                })
            }
            Value::Dictionary(d) => {
                let members: Vec<_> = d.iter().map(|(k, v)| {
                    let k = walk(k);
                    let v = walk(v);
                    quote!(___d.insert(#k, #v))
                }).collect();
                quote!({
                    let mut ___d = #V_::Map::new();
                    #(#members;)*
                    #V_::Value::from(___d).wrap()
                })
            }
            Value::Embedded(_) =>
                panic!("Embedded values in compile-time Preserves templates not (yet?) supported"),
        }
    }
}

fn compile_pattern(v: &IOValue) -> TokenStream {
    #[allow(non_snake_case)]
    let P_: TokenStream = quote!(syndicate::schemas::dataspace_patterns);
    #[allow(non_snake_case)]
    let V_: TokenStream = quote!(syndicate::value);
    #[allow(non_snake_case)]
    let MapFromIterator_: TokenStream = quote!(<#V_::Map<_, _> as std::iter::FromIterator<_>>::from_iter);

    match v.value() {
        Value::Symbol(s) => match analyze_symbol(&s, true) {
            SymbolVariant::Binder(_) =>
                quote!(#P_::Pattern::DBind(Box::new(#P_::DBind {
                    pattern: #P_::Pattern::DDiscard(Box::new(#P_::DDiscard))
                }))),
            SymbolVariant::Discard =>
                quote!(#P_::Pattern::DDiscard(Box::new(#P_::DDiscard))),
            SymbolVariant::Substitution(s) =>
                lit(Ident::new(s, Span::call_site())),
            SymbolVariant::Normal(_) =>
                lit(ValueCompiler::for_patterns().compile(v)),
        }
        Value::Record(r) => {
            let arity = r.arity() as u128;
            match r.label().value().as_symbol() {
                None => panic!("Record labels in patterns must be symbols"),
                Some(label) =>
                    if label.starts_with("$") && arity == 1 {
                        let nested = compile_pattern(&r.fields()[0]);
                        quote!(#P_::Pattern::DBind(Box::new(#P_::DBind {
                            pattern: #nested
                        })))
                    } else {
                        let label_stx = if label.starts_with("=") {
                            let id = Ident::new(&label[1..], Span::call_site());
                            quote!(#id)
                        } else {
                            quote!(#V_::Value::symbol(#label).wrap())
                        };
                        let members = compile_sequence_members(r.fields());
                        quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Rec {
                            ctor: Box::new(#P_::CRec {
                                label: #label_stx,
                                arity: #arity .into(),
                            }),
                            members: #MapFromIterator_(vec![#(#members),*])
                        })))
                    }
            }
        }
        Value::Sequence(vs) => {
            let arity = vs.len() as u128;
            let members = compile_sequence_members(vs);
            quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Arr {
                ctor: Box::new(#P_::CArr {
                    arity: #arity .into(),
                }),
                members: #MapFromIterator_(vec![#(#members),*])
            })))
        }
        Value::Set(_) =>
            panic!("Cannot match sets in patterns"),
        Value::Dictionary(d) => {
            let members = d.iter().map(|(k, v)| {
                let k = ValueCompiler::for_patterns().compile(k);
                let v = compile_pattern(v);
                quote!((#k, #v))
            }).collect::<Vec<_>>();
            quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Dict {
                ctor: Box::new(#P_::CDict),
                members: #MapFromIterator_(vec![#(#members),*])
            })))
        }
        _ => lit(ValueCompiler::for_patterns().compile(v)),
    }
}

#[proc_macro]
pub fn pattern_str(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    if let Lit::Str(s) = parse_macro_input!(src as ExprLit).lit {
        match iovalue_from_str(&s.value()) {
            Ok(v) => {
                let e = compile_pattern(&v);
                // println!("{:#}", &e);
                return e.into();
            }
            Err(_) => (),
        }
    }

    panic!("Expected literal string containing the pattern and no more");
}

#[proc_macro]
pub fn template(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    if let Lit::Str(s) = parse_macro_input!(src as ExprLit).lit {
        match iovalue_from_str(&s.value()) {
            Ok(v) => {
                let e = ValueCompiler::for_templates().compile(&v);
                // println!("{:#}", &e);
                return e.into();
            }
            Err(_) => (),
        }
    }

    panic!("Expected literal string containing the template and no more");
}

//---------------------------------------------------------------------------

#[proc_macro]
pub fn pattern(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    pat::pattern(src)
}

//---------------------------------------------------------------------------

#[proc_macro]
pub fn during(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    dur::during(src)
}
