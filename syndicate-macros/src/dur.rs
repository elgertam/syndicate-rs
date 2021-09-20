use proc_macro2::Span;

use quote::quote_spanned;

use syn::parse_macro_input;
use syn::Expr;
use syn::Ident;
use syn::LitInt;
use syn::Token;
use syn::Type;
use syn::parse::Error;
use syn::parse::Parse;
use syn::parse::ParseStream;

use crate::stx::Stx;
use crate::pat;

#[derive(Debug)]
struct During {
    turn_stx: Expr,
    ds_stx: Expr,
    lang_stx: Expr,
    pat_stx: Stx,
    body_stx: Expr,
}

fn comma_parse<T: Parse>(input: ParseStream) -> syn::parse::Result<T> {
    let _: Token![,] = input.parse()?;
    input.parse()
}

impl Parse for During {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        Ok(During {
            turn_stx: input.parse()?,
            ds_stx: comma_parse(input)?,
            lang_stx: comma_parse(input)?,
            pat_stx: comma_parse(input)?,
            body_stx: comma_parse(input)?,
        })
    }
}

impl During {
    fn bindings(&self) -> (Vec<Ident>, Vec<Type>, Vec<LitInt>) {
        let mut ids = vec![];
        let mut tys = vec![];
        let mut indexes = vec![];
        for (i, (maybe_id, ty)) in self.pat_stx.bindings().into_iter().enumerate() {
            if let Some(id) = maybe_id {
                indexes.push(LitInt::new(&i.to_string(), id.span()));
                ids.push(id);
                tys.push(ty);
            }
        }
        (ids, tys, indexes)
    }
}

pub fn during(src: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let d = parse_macro_input!(src as During);
    let During { turn_stx, ds_stx, lang_stx, pat_stx, body_stx } = &d;
    let (varname_stx, type_stx, index_stx) = d.bindings();
    let binding_count = varname_stx.len();
    let pat_stx_expr = match pat::to_pattern_expr(pat_stx) {
        Ok(e) => e,
        Err(e) => return Error::new(Span::call_site(), e).to_compile_error().into(),
    };
    (quote_spanned!{Span::mixed_site()=> {
        let __ds = #ds_stx.clone();
        let __lang = #lang_stx;
        let monitor = syndicate::during::entity(())
            .on_asserted_facet(move |_, t, captures: syndicate::actor::AnyValue| {
                if let Some(captures) = {
                    use syndicate::value::NestedValue;
                    use syndicate::value::Value;
                    captures.value().as_sequence()
                }{
                    if captures.len() == #binding_count {
                        #(let #varname_stx: #type_stx = match {
                            use syndicate::preserves_schema::Codec;
                            __lang.parse(&captures[#index_stx])
                        } {
                            Ok(v) => v,
                            Err(_) => return Ok(()),
                        };)*
                        return (#body_stx)(t);
                    }
                }
                Ok(())
            })
            .create_cap(#turn_stx);
        __ds.assert(#turn_stx, __lang, &syndicate::schemas::dataspace::Observe {
            pattern: #pat_stx_expr,
            observer: monitor,
        });
    }}).into()
}
