use syndicate::value::IOValue;
use syndicate::value::NestedValue;
use syndicate::value::Value;
use syndicate::value::text::iovalue_from_str;

extern crate proc_macro;
use proc_macro::Span;
use proc_macro::TokenStream;

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
            syndicate::schemas::dataspace_patterns::DLit { value: #e }))).into()
}

fn compile_pattern(v: &IOValue) -> TokenStream {
    match v.value() {
        Value::Boolean(b) => lit(quote!(syndicate::value::Value::from(#b).wrap())),
        Value::Float(f) => {
            let f = f.0;
            lit(quote!(syndicate::value::Value::from(#f).wrap()))
        }
        Value::Double(d) => {
            let d = d.0;
            lit(quote!(syndicate::value::Value::from(#d).wrap()))
        }
        Value::SignedInteger(i) => {
            let i = i128::try_from(i).expect("Literal integer out-of-range");
            lit(quote!(syndicate::value::Value::from(#i).wrap()))
        }
        Value::String(s) => lit(quote!(syndicate::value::Value::from(#s).wrap())),
        Value::ByteString(bs) => {
            let bs = LitByteStr::new(bs, Span::call_site().into());
            lit(quote!(syndicate::value::Value::from(#bs).wrap()))
        }
        Value::Symbol(s) => match s.as_str() {
            "$" => quote!(
                syndicate::schemas::dataspace_patterns::Pattern::DBind(Box::new(
                    syndicate::schemas::dataspace_patterns::DBind {
                        pattern: syndicate::schemas::dataspace_patterns::Pattern::DDiscard(Box::new(
                            syndicate::schemas::dataspace_patterns::DDiscard))
                    }))).into(),
            "_" => quote!(
                syndicate::schemas::dataspace_patterns::Pattern::DDiscard(Box::new(
                    syndicate::schemas::dataspace_patterns::DDiscard))).into(),
            _ => if s.starts_with("=") {
                let id = Ident::new(&s[1..], Span::call_site().into());
                lit(quote!(#id))
            } else {
                // let s = LitStr::new(s, Span::call_site().into());
                lit(quote!(syndicate::value::Value::symbol(#s).wrap()))
            },
        }
        Value::Record(r) => {
            let arity = r.arity() as u128;
            match r.label().value().as_symbol() {
                None => panic!("Record labels in patterns must be symbols"),
                Some(label) => {
                    let mut i = 0;
                    let members = r.fields().iter().map(
                        |f| {
                            let p: proc_macro2::TokenStream = compile_pattern(f).into();
                            let result = quote!((#i .into(), #p));
                            i += 1;
                            result
                        }).collect::<Vec<_>>();
                    quote!(
                        syndicate::schemas::dataspace_patterns::Pattern::DCompound(Box::new(
                            syndicate::schemas::dataspace_patterns::DCompound::Rec {
                                ctor: Box::new(syndicate::schemas::dataspace_patterns::CRec {
                                    label: syndicate::value::Value::symbol(#label).wrap(),
                                    arity: #arity .into(),
                                }),
                                members: syndicate::value::Map::from_iter(vec![#(#members),*])
                            }))).into()
                }
            }
        }
        // Value::Sequence(vs) => ,
        // Value::Set(_) => panic!("Cannot match sets in patterns"),
        // Value::Dictionary(d) => ,
        // Value::Embedded(e) => panic!("aiee"),
        _ => todo!(),
    }
}

#[proc_macro]
pub fn pattern(src: TokenStream) -> TokenStream {
    if let Lit::Str(s) = parse_macro_input!(src as ExprLit).lit {
        match iovalue_from_str(&s.value()) {
            Ok(v) => {
                let e = compile_pattern(&v);
                // println!("{:#}", &e);
                return e;
            }
            Err(_) => (),
        }
    }

    panic!("Expected literal string containing the pattern and no more");
}
