#![feature(proc_macro_span)]

use syndicate::preserves::Atom;
use syndicate::preserves::AtomClass;
use syndicate::preserves::CompoundClass;
use syndicate::preserves::IOValue;
use syndicate::preserves::Value;
use syndicate::preserves::ValueClass;
use syndicate::preserves::read_iovalue_text;

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
    #[allow(dead_code)] // otherwise we get 'warning: field `0` is never read'
    Binder(&'a str),
    Substitution(&'a str),
    Discard,
}

fn compile_sequence_members(vs: Vec<Value<IOValue>>) -> Vec<TokenStream> {
    vs.into_iter().enumerate().map(|(i, f)| {
        let p = compile_pattern(f);
        quote!((syndicate::preserves::Value::new(#i), #p))
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

    fn compile(&self, v: Value<IOValue>) -> TokenStream {
        #[allow(non_snake_case)]
        let V_: TokenStream = quote!(syndicate::preserves);

        let walk = |w| self.compile(w);

        match v.value_class() {
            ValueClass::Atomic(_) => match v.as_atom().unwrap() {
                Atom::Boolean(b) =>
                    quote!(#V_::Value::new(#b)),
                Atom::Double(d) =>
                    quote!(#V_::Value::new(#d)),
                Atom::SignedInteger(i) => {
                    let i = i128::try_from(i.as_ref()).expect("Literal integer out-of-range");
                    quote!(#V_::Value::new(#i))
                }
                Atom::String(s) =>
                    quote!(#V_::Value::new(#s)),
                Atom::ByteString(bs) => {
                    let bs = LitByteStr::new(bs.as_ref(), Span::call_site());
                    quote!(#V_::Value::new(#bs))
                }
                Atom::Symbol(s) => match analyze_symbol(&s, self.allow_binding_and_substitution) {
                    SymbolVariant::Normal(s) =>
                        quote!(#V_::Value::symbol(#s)),
                    SymbolVariant::Binder(_) |
                    SymbolVariant::Discard =>
                        panic!("Binding/Discard not supported here"),
                    SymbolVariant::Substitution(s) => {
                        let i = Ident::new(s, Span::call_site());
                        quote!(#i)
                    }
                }
            },
            ValueClass::Compound(c) => match c {
                CompoundClass::Record => {
                    let mut vs = vec![walk(v.label())];
                    for f in v.iter() { vs.push(walk(f)); }
                    quote!(#V_::Value::new(#V_::Record::_from_vec(vec![#(#vs),*])))
                }
                CompoundClass::Sequence => {
                    let vs: Vec<_> = v.iter().map(walk).collect();
                    quote!(#V_::Value::new(vec![#(#vs),*]))
                }
                CompoundClass::Set => {
                    let vs: Vec<_> = v.iter().map(walk).collect();
                    quote!({
                        let mut ___s = #V_::Set::new();
                        #(___s.insert(#vs);)*
                        #V_::Value::new(___s)
                    })
                }
                CompoundClass::Dictionary => {
                    let members: Vec<_> = v.entries().map(|(k, v)| {
                        let k = walk(k);
                        let v = walk(v);
                        quote!(___d.insert(#k, #v))
                    }).collect();
                    quote!({
                        let mut ___d = #V_::Map::new();
                        #(#members;)*
                        #V_::Value::new(___d)
                    })
                }
            },
            ValueClass::Embedded =>
                panic!("Embedded values in compile-time Preserves templates not (yet?) supported"),
        }
    }
}

fn compile_pattern(v: Value<IOValue>) -> TokenStream {
    #[allow(non_snake_case)]
    let P_: TokenStream = quote!(syndicate::schemas::dataspace_patterns);
    #[allow(non_snake_case)]
    let V_: TokenStream = quote!(syndicate::preserves);
    #[allow(non_snake_case)]
    let MapFrom_: TokenStream = quote!(<#V_::Map<_, _>>::from);

    match v.value_class() {
        ValueClass::Atomic(AtomClass::Symbol) => match analyze_symbol(v.as_symbol().unwrap().as_ref(), true) {
            SymbolVariant::Binder(_) =>
                quote!(#P_::Pattern::Bind{ pattern: Box::new(#P_::Pattern::Discard) }),
            SymbolVariant::Discard =>
                quote!(#P_::Pattern::Discard),
            SymbolVariant::Substitution(s) =>
                lit(Ident::new(s, Span::call_site())),
            SymbolVariant::Normal(_) =>
                lit(ValueCompiler::for_patterns().compile(v)),
        }
        ValueClass::Compound(c) => match c {
            CompoundClass::Record => {
                match v.label().as_symbol() {
                    None => panic!("Record labels in patterns must be symbols"),
                    Some(label) =>
                        if label.starts_with("$") && v.len() == 1 {
                            let nested = compile_pattern(v.index(0));
                            quote!(#P_::Pattern::Bind{ pattern: Box::new(#nested) })
                        } else {
                            let label_stx = if label.starts_with("=") {
                                let id = Ident::new(&label[1..], Span::call_site());
                                quote!(#id)
                            } else {
                                quote!(#V_::Value::symbol(#label))
                            };
                            let members = compile_sequence_members(v.iter().collect());
                            quote!(#P_::Pattern::Group {
                                type_: Box::new(#P_::GroupType::Rec { label: #label_stx }),
                                entries: #MapFrom_([#(#members),*]),
                            })
                        }
                }
            }
            CompoundClass::Sequence => {
                let members = compile_sequence_members(v.iter().collect());
                quote!(#P_::Pattern::Group {
                    type_: Box::new(#P_::GroupType::Arr),
                    entries: #MapFrom_([#(#members),*]),
                })
            }
            CompoundClass::Set =>
                panic!("Cannot match sets in patterns"),
            CompoundClass::Dictionary => {
                let members = v.entries().map(|(k, v)| {
                    let k = ValueCompiler::for_patterns().compile(k);
                    let v = compile_pattern(v);
                    quote!((#k, #v))
                }).collect::<Vec<_>>();
                quote!(#P_::Pattern::Group {
                    type_: Box::new(#P_::GroupType::Dict),
                    entries: #MapFrom_([#(#members),*]),
                })
            }
        }
        _ => lit(ValueCompiler::for_patterns().compile(v)),
    }
}

#[proc_macro]
pub fn pattern_str(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    if let Lit::Str(s) = parse_macro_input!(src as ExprLit).lit {
        match read_iovalue_text(&s.value(), true) {
            Ok(v) => {
                let e = compile_pattern(v.into());
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
        match read_iovalue_text(&s.value(), true) {
            Ok(v) => {
                let e = ValueCompiler::for_templates().compile(v.into());
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

#[proc_macro]
pub fn on_message(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    dur::on_message(src)
}
