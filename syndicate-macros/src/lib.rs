use syndicate::value::IOValue;
use syndicate::value::NestedValue;
use syndicate::value::Value;
use syndicate::value::text::iovalue_from_str;

extern crate proc_macro;

use proc_macro2::Span;
use proc_macro2::TokenStream;

use quote::ToTokens;
use quote::quote;

use std::convert::TryFrom;

use syn::parse_macro_input;
use syn::ExprLit;
use syn::Ident;
use syn::Lit;
use syn::LitByteStr;

fn lit<T: ToTokens>(e: T) -> TokenStream {
    quote!(
        syndicate::schemas::dataspace_patterns::Pattern::DLit(Box::new(
            syndicate::schemas::dataspace_patterns::DLit { value: #e })))
}

fn compile_sequence_members(vs: &[IOValue]) -> Vec<TokenStream> {
    let mut i = 0;
    vs.iter().map(|f| {
        let p = compile_pattern(f);
        let result = quote!((#i .into(), #p));
        i += 1;
        result
    }).collect::<Vec<_>>()
}

fn compile_value(v: &IOValue) -> TokenStream {
    #[allow(non_snake_case)]
    let V_: TokenStream = quote!(syndicate::value);

    match v.value() {
        Value::Boolean(b) =>
            lit(quote!(#V_::Value::from(#b).wrap())),
        Value::Float(f) => {
            let f = f.0;
            lit(quote!(#V_::Value::from(#f).wrap()))
        }
        Value::Double(d) => {
            let d = d.0;
            lit(quote!(#V_::Value::from(#d).wrap()))
        }
        Value::SignedInteger(i) => {
            let i = i128::try_from(i).expect("Literal integer out-of-range");
            lit(quote!(#V_::Value::from(#i).wrap()))
        }
        Value::String(s) =>
            lit(quote!(#V_::Value::from(#s).wrap())),
        Value::ByteString(bs) => {
            let bs = LitByteStr::new(bs, Span::call_site());
            lit(quote!(#V_::Value::from(#bs).wrap()))
        }
        Value::Symbol(s) =>
            lit(quote!(#V_::Value::symbol(#s).wrap())),
        Value::Record(r) => {
            let arity = r.arity();
            let label = compile_value(r.label());
            let fs: Vec<_> = r.fields().iter().map(compile_value).collect();
            quote!({
                let ___r = #V_::Value::record(#label, #arity);
                #(___r.fields_vec_mut().push(#fs);)*
                ___r.finish()
            })
        }
        Value::Sequence(vs) => {
            let vs: Vec<_> = vs.iter().map(compile_value).collect();
            quote!(#V_::Value::from(vec![#(#vs),*]))
        }
        Value::Set(vs) => {
            let vs: Vec<_> = vs.iter().map(compile_value).collect();
            quote!({
                let ___s = #V_::Set::new();
                #(___s.insert(#vs);)*
                #V_::Value::from(___s)
            })
        }
        Value::Dictionary(d) => {
            let members: Vec<_> = d.iter().map(|(k, v)| {
                let k = compile_value(k);
                let v = compile_pattern(v);
                quote!(___d.insert(#k, #v))
            }).collect();
            quote!({
                let ___d = #V_::Map::new();
                #(#members;)*
                #V_::Value::from(___d)
            })
        }
        Value::Embedded(_) =>
            panic!("Embedded values in patterns not (yet?) supported"),
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
        Value::Symbol(s) => match s.as_str() {
            "$" =>
                quote!(#P_::Pattern::DBind(Box::new(#P_::DBind {
                    pattern: #P_::Pattern::DDiscard(Box::new(#P_::DDiscard))
                }))),
            "_" =>
                quote!(#P_::Pattern::DDiscard(Box::new(#P_::DDiscard))),
            _ =>
                if s.starts_with("=") {
                    lit(Ident::new(&s[1..], Span::call_site()))
                } else {
                    lit(compile_value(v))
                },
        }
        Value::Record(r) => {
            let arity = r.arity() as u128;
            match r.label().value().as_symbol() {
                None => panic!("Record labels in patterns must be symbols"),
                Some(label) => {
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
                let k = compile_value(k);
                let v = compile_pattern(v);
                quote!((#k, #v))
            }).collect::<Vec<_>>();
            quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Dict {
                ctor: Box::new(#P_::CDict),
                members: #MapFromIterator_(vec![#(#members),*])
            })))
        }
        _ => lit(compile_value(v)),
    }
}

#[proc_macro]
pub fn pattern(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
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
