use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;

use quote::quote;

use std::convert::TryFrom;

use syn::LitByteStr;

use syndicate::preserves::Atom;
use syndicate::preserves::CompoundClass;
use syndicate::preserves::IOValue;
use syndicate::preserves::Value;
use syndicate::preserves::ValueClass;

use crate::stx::Stx;

pub fn emit_record(label: TokenStream2, fs: Vec<TokenStream2>) -> TokenStream2 {
    quote!(syndicate::preserves::Value::record(#label, vec![#(#fs),*]))
}

pub fn emit_seq(vs: Vec<TokenStream2>) -> TokenStream2 {
    quote!(syndicate::preserves::Value::new(vec![#(#vs),*]))
}

pub fn emit_set(vs: Vec<TokenStream2>) -> TokenStream2 {
    quote!({
        let mut ___s = syndicate::preserves::Set::new();
        #(___s.insert(#vs);)*
        syndicate::preserves::Value::new(___s)
    })
}

pub fn emit_dict(d: Vec<(TokenStream2, TokenStream2)>) -> TokenStream2 {
    let members: Vec<_> = d.into_iter().map(|(k, v)| quote!(___d.insert(#k, #v))).collect();
    quote!({
        let mut ___d = syndicate::preserves::Map::new();
        #(#members;)*
        syndicate::preserves::Value::new(___d)
    })
}

pub fn value_to_value_expr(v: Value<IOValue>) -> TokenStream2 {
    #[allow(non_snake_case)]
    let V_: TokenStream2 = quote!(syndicate::preserves);

    match v.value_class() {
        ValueClass::Atomic(_) => match v.as_atom().unwrap() {
            Atom::Boolean(b) => quote!(#V_::Value::new(#b)),
            Atom::Double(d) => quote!(#V_::Value::new(#d)),
            Atom::SignedInteger(i) => {
                let i = i128::try_from(i.as_ref()).expect("Literal integer out-of-range");
                quote!(#V_::Value::new(#i))
            }
            Atom::String(s) => quote!(#V_::Value::new(#s)),
            Atom::ByteString(bs) => {
                let bs = LitByteStr::new(bs.as_ref(), Span::call_site());
                quote!(#V_::Value::bytes(#bs))
            }
            Atom::Symbol(s) => quote!(#V_::Value::symbol(#s)),
        },
        ValueClass::Compound(c) => match c {
            CompoundClass::Record =>
                emit_record(value_to_value_expr(v.label()),
                            v.iter().map(value_to_value_expr).collect()),
            CompoundClass::Sequence =>
                emit_seq(v.iter().map(value_to_value_expr).collect()),
            CompoundClass::Set =>
                emit_set(v.iter().map(value_to_value_expr).collect()),
            CompoundClass::Dictionary =>
                emit_dict(v.entries().map(|(k, v)| (value_to_value_expr(k), value_to_value_expr(v))).collect()),
        }
        ValueClass::Embedded =>
            panic!("Embedded values in compile-time Preserves templates not (yet?) supported"),
    }
}

pub fn to_value_expr(stx: &Stx) -> Result<TokenStream2, &'static str> {
    match stx {
        Stx::Atom(v) => Ok(value_to_value_expr(v.clone())),
        Stx::Binder(_, _, _) => Err("Cannot use binder in literal value"),
        Stx::Discard => Err("Cannot use discard in literal value"),
        Stx::Subst(e) => Ok(e.clone().into()),

        Stx::Rec(l, fs) =>
            Ok(emit_record(to_value_expr(&*l)?,
                           fs.into_iter().map(to_value_expr).collect::<Result<Vec<_>, _>>()?)),
        Stx::Seq(vs) =>
            Ok(emit_seq(vs.into_iter().map(to_value_expr).collect::<Result<Vec<_>, _>>()?)),
        Stx::Set(vs) =>
            Ok(emit_set(vs.into_iter().map(to_value_expr).collect::<Result<Vec<_>, _>>()?)),
        Stx::Dict(kvs) =>
            Ok(emit_dict(kvs.into_iter()
                         .map(|(k, v)| Ok((to_value_expr(k)?, to_value_expr(v)?)))
                         .collect::<Result<Vec<_>, &str>>()?)),
    }
}
