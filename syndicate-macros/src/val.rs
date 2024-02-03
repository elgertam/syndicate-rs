use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;

use quote::quote;

use std::convert::TryFrom;

use syn::LitByteStr;

use syndicate::value::IOValue;
use syndicate::value::NestedValue;
use syndicate::value::Value;

use crate::stx::Stx;

pub fn emit_record(label: TokenStream2, fs: &[TokenStream2]) -> TokenStream2 {
    let arity = fs.len();
    quote!({
        let mut ___r = syndicate::value::Value::record(#label, #arity);
        #(___r.fields_vec_mut().push(#fs);)*
        ___r.finish().wrap()
    })
}

pub fn emit_seq(vs: &[TokenStream2]) -> TokenStream2 {
    quote!(syndicate::value::Value::from(vec![#(#vs),*]).wrap())
}

pub fn emit_set(vs: &[TokenStream2]) -> TokenStream2 {
    quote!({
        let mut ___s = syndicate::value::Set::new();
        #(___s.insert(#vs);)*
        syndicate::value::Value::from(___s).wrap()
    })
}

pub fn emit_dict<'a, I: Iterator<Item = (TokenStream2, TokenStream2)>>(d: I) -> TokenStream2 {
    let members: Vec<_> = d.map(|(k, v)| quote!(___d.insert(#k, #v))).collect();
    quote!({
        let mut ___d = syndicate::value::Map::new();
        #(#members;)*
        syndicate::value::Value::from(___d).wrap()
    })
}

pub fn value_to_value_expr(v: &IOValue) -> TokenStream2 {
    #[allow(non_snake_case)]
    let V_: TokenStream2 = quote!(syndicate::value);

    match v.value() {
        Value::Boolean(b) =>
            quote!(#V_::Value::from(#b).wrap()),
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
            quote!(#V_::Value::ByteString(#bs.to_vec()).wrap())
        }
        Value::Symbol(s) =>
            quote!(#V_::Value::symbol(#s).wrap()),
        Value::Record(r) =>
            emit_record(value_to_value_expr(r.label()),
                        &r.fields().iter().map(value_to_value_expr).collect::<Vec<_>>()),
        Value::Sequence(vs) =>
            emit_seq(&vs.iter().map(value_to_value_expr).collect::<Vec<_>>()),
        Value::Set(vs) =>
            emit_set(&vs.iter().map(value_to_value_expr).collect::<Vec<_>>()),
        Value::Dictionary(d) =>
            emit_dict(d.into_iter().map(|(k, v)| (value_to_value_expr(k), value_to_value_expr(v)))),
        Value::Embedded(_) =>
            panic!("Embedded values in compile-time Preserves templates not (yet?) supported"),
    }
}

pub fn to_value_expr(stx: &Stx) -> Result<TokenStream2, &'static str> {
    match stx {
        Stx::Atom(v) => Ok(value_to_value_expr(&v)),
        Stx::Binder(_, _, _) => Err("Cannot use binder in literal value"),
        Stx::Discard => Err("Cannot use discard in literal value"),
        Stx::Subst(e) => Ok(e.clone().into()),

        Stx::Rec(l, fs) =>
            Ok(emit_record(to_value_expr(&*l)?,
                           &fs.into_iter().map(to_value_expr).collect::<Result<Vec<_>,_>>()?)),
        Stx::Seq(vs) =>
            Ok(emit_seq(&vs.into_iter().map(to_value_expr).collect::<Result<Vec<_>,_>>()?)),
        Stx::Set(vs) =>
            Ok(emit_set(&vs.into_iter().map(to_value_expr).collect::<Result<Vec<_>,_>>()?)),
        Stx::Dict(kvs) =>
            Ok(emit_dict(kvs.into_iter()
                         .map(|(k, v)| Ok((to_value_expr(k)?, to_value_expr(v)?)))
                         .collect::<Result<Vec<_>,&'static str>>()?
                         .into_iter())),
    }
}
