use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

use quote::ToTokens;
use quote::quote;

use syn::parse_macro_input;

use crate::stx::Stx;
use crate::val::to_value_expr;
use crate::val::value_to_value_expr;

pub fn lit<T: ToTokens>(e: T) -> TokenStream2 {
    quote!(syndicate::pattern::lift_literal(#e))
}

fn compile_sequence_members(stxs: &Vec<Stx>) -> Result<Vec<TokenStream2>, &'static str> {
    stxs.iter().map(|stx| {
        // let p = to_pattern_expr(stx)?;
        // Ok(quote!(#p))
        to_pattern_expr(stx)
    }).collect()
}

pub fn to_pattern_expr(stx: &Stx) -> Result<TokenStream2, &'static str> {
    #[allow(non_snake_case)]
    let P_: TokenStream2 = quote!(syndicate::schemas::dataspace_patterns);
    #[allow(non_snake_case)]
    let V_: TokenStream2 = quote!(syndicate::value);
    #[allow(non_snake_case)]
    let MapFromIterator_: TokenStream2 = quote!(<#V_::Map<_, _> as std::iter::FromIterator<_>>::from_iter);

    match stx {
        Stx::Atom(v) =>
            Ok(lit(value_to_value_expr(&v))),
        Stx::Binder(_, maybe_ty, maybe_pat) => {
            let inner_pat_expr = match maybe_pat {
                Some(p) => to_pattern_expr(&*p)?,
                None => match maybe_ty {
                    Some(ty) => quote!(#ty::wildcard_dataspace_pattern()),
                    None => to_pattern_expr(&Stx::Discard)?,
                }
            };
            Ok(quote!(#P_::Pattern::DBind(Box::new(#P_::DBind { pattern: #inner_pat_expr }))))
        }
        Stx::Subst(e) =>
            Ok(lit(e)),
        Stx::Discard =>
            Ok(quote!(#P_::Pattern::DDiscard(Box::new(#P_::DDiscard)))),

        Stx::Rec(l, fs) => {
            let label = to_value_expr(&*l)?;
            let members = compile_sequence_members(fs)?;
            Ok(quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Rec {
                label: #label,
                fields: vec![#(#members),*],
            }))))
        },
        Stx::Seq(stxs) => {
            let members = compile_sequence_members(stxs)?;
            Ok(quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Arr {
                items: vec![#(#members),*],
            }))))
        }
        Stx::Set(_stxs) =>
            Err("Set literals not supported in patterns"),
        Stx::Dict(d) => {
            let members = d.iter().map(|(k, v)| {
                let k = to_value_expr(k)?;
                let v = to_pattern_expr(v)?;
                Ok(quote!((#k, #v)))
            }).collect::<Result<Vec<_>, &'static str>>()?;
            Ok(quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Dict {
                entries: #MapFromIterator_(vec![#(#members),*])
            }))))
        }
    }
}

pub fn pattern(src: TokenStream) -> TokenStream {
    let src2 = src.clone();
    let e = to_pattern_expr(&parse_macro_input!(src2 as Stx))
        .expect("Cannot compile pattern").into();
    // println!("\n{:#} -->\n{:#}\n", &src, &e);
    e
}
