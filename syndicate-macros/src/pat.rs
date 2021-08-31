use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

use quote::ToTokens;
use quote::quote;

use syn::parse_macro_input;

use crate::stx::Stx;
use crate::val::emit_set;
use crate::val::to_value_expr;
use crate::val::value_to_value_expr;

pub fn lit<T: ToTokens>(e: T) -> TokenStream2 {
    quote!(
        syndicate::schemas::dataspace_patterns::Pattern::DLit(Box::new(
            syndicate::schemas::dataspace_patterns::DLit { value: #e })))
}

fn compile_sequence_members(stxs: Vec<Stx>) -> Result<Vec<TokenStream2>, &'static str> {
    stxs.into_iter().enumerate().map(|(i, stx)| {
        let p = to_pattern_expr(stx)?;
        Ok(quote!((#i .into(), #p)))
    }).collect()
}

fn to_pattern_expr(stx: Stx) -> Result<TokenStream2, &'static str> {
    #[allow(non_snake_case)]
    let P_: TokenStream2 = quote!(syndicate::schemas::dataspace_patterns);
    #[allow(non_snake_case)]
    let V_: TokenStream2 = quote!(syndicate::value);
    #[allow(non_snake_case)]
    let MapFromIterator_: TokenStream2 = quote!(<#V_::Map<_, _> as std::iter::FromIterator<_>>::from_iter);

    match stx {
        Stx::Atom(v) =>
            Ok(lit(value_to_value_expr(&v))),
        Stx::Binder(_, p) => {
            let pe = to_pattern_expr(*p)?;
            Ok(quote!(#P_::Pattern::DBind(Box::new(#P_::DBind { pattern: #pe }))))
        }
        Stx::Subst(e) =>
            Ok(lit(e)),
        Stx::Discard =>
            Ok(quote!(#P_::Pattern::DDiscard(Box::new(#P_::DDiscard)))),

        Stx::Ctor1(_, _) => todo!(),
        Stx::CtorN(_, _) => todo!(),

        Stx::Rec(l, fs) => {
            let arity = fs.len() as u128;
            let label = to_value_expr(*l)?;
            let members = compile_sequence_members(fs)?;
            Ok(quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Rec {
                ctor: Box::new(#P_::CRec { label: #label, arity: #arity .into() }),
                members: #MapFromIterator_(vec![#(#members),*])
            }))))
        },
        Stx::Seq(stxs) => {
            let arity = stxs.len() as u128;
            let members = compile_sequence_members(stxs)?;
            Ok(quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Arr {
                ctor: Box::new(#P_::CArr { arity: #arity .into() }),
                members: #MapFromIterator_(vec![#(#members),*])
            }))))
        }
        Stx::Set(stxs) =>
            Ok(lit(emit_set(&stxs.into_iter().map(to_value_expr).collect::<Result<Vec<_>,_>>()?))),
        Stx::Dict(d) => {
            let members = d.into_iter().map(|(k, v)| {
                let k = to_value_expr(k)?;
                let v = to_pattern_expr(v)?;
                Ok(quote!((#k, #v)))
            }).collect::<Result<Vec<_>, &'static str>>()?;
            Ok(quote!(#P_::Pattern::DCompound(Box::new(#P_::DCompound::Dict {
                ctor: Box::new(#P_::CDict),
                members: #MapFromIterator_(vec![#(#members),*])
            }))))
        }
    }
}

pub fn pattern(src: TokenStream) -> TokenStream {
    let src2 = src.clone();
    let e = to_pattern_expr(parse_macro_input!(src2 as Stx)).expect("Cannot compile pattern").into();
    // println!("\n{:#} -->\n{:#}\n", &src, &e);
    e
}
