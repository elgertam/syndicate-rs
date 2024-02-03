use proc_macro2::Delimiter;
use proc_macro2::LineColumn;
use proc_macro2::Span;
use proc_macro2::TokenStream;

use syn::ExprLit;
use syn::Ident;
use syn::Lit;
use syn::Result;
use syn::Type;
use syn::buffer::Cursor;
use syn::parse::Error;
use syn::parse::Parse;
use syn::parse::Parser;
use syn::parse::ParseStream;
use syn::parse_str;

use syndicate::value::Double;
use syndicate::value::IOValue;
use syndicate::value::NestedValue;

#[derive(Debug, Clone)]
pub enum Stx {
    Atom(IOValue),
    Binder(Option<Ident>, Option<Type>, Option<Box<Stx>>),
    Discard,
    Subst(TokenStream),
    Rec(Box<Stx>, Vec<Stx>),
    Seq(Vec<Stx>),
    Set(Vec<Stx>),
    Dict(Vec<(Stx, Stx)>),
}

impl Parse for Stx {
    fn parse(input: ParseStream) -> Result<Self> {
        input.step(|c| parse1(*c))
    }
}

impl Stx {
    pub fn bindings(&self) -> Vec<(Option<Ident>, Type)> {
        let mut bs = vec![];
        self._bindings(&mut bs);
        bs
    }

    fn _bindings(&self, bs: &mut Vec<(Option<Ident>, Type)>) {
        match self {
            Stx::Atom(_) | Stx::Discard | Stx::Subst(_) => (),
            Stx::Binder(id, ty, pat) => {
                bs.push((id.clone(),
                         ty.clone().unwrap_or_else(
                             || parse_str("syndicate::actor::AnyValue").unwrap())));
                if let Some(p) = pat {
                    p._bindings(bs);
                }
            },
            Stx::Rec(l, fs) => {
                l._bindings(bs);
                fs.iter().for_each(|f| f._bindings(bs));
            },
            Stx::Seq(vs) => vs.iter().for_each(|v| v._bindings(bs)),
            Stx::Set(vs) => vs.iter().for_each(|v| v._bindings(bs)),
            Stx::Dict(kvs) => kvs.iter().for_each(|(_k, v)| v._bindings(bs)),
        }
    }
}

fn punct_char(c: Cursor) -> Option<(char, Cursor)> {
    c.punct().map(|(p, c)| (p.as_char(), c))
}

fn start_pos(s: Span) -> LineColumn {
    // We would like to write
    //     s.start()
    // here, but until [1] is fixed (perhaps via [2]), we have to go the unsafe route
    // and assume we are in procedural macro context.
    // [1]: https://github.com/dtolnay/proc-macro2/issues/402
    // [2]: https://github.com/dtolnay/proc-macro2/pull/407
    let u = s.unwrap().start();
    LineColumn { column: u.column(), line: u.line() }
}

fn end_pos(s: Span) -> LineColumn {
    // See start_pos
    let u = s.unwrap().end();
    LineColumn { column: u.column(), line: u.line() }
}

fn parse_id(mut c: Cursor) -> Result<(String, Cursor)> {
    let mut id = String::new();
    let mut prev_pos = start_pos(c.span());
    loop {
        if c.eof() || start_pos(c.span()) != prev_pos {
            return Ok((id, c));
        } else if let Some((p, next)) = c.punct() {
            match p.as_char() {
                '<' | '>' | '(' | ')' | '{' | '}' | '[' | ']' | ',' | ':' => return Ok((id, c)),
                ch => {
                    id.push(ch);
                    prev_pos = end_pos(c.span());
                    c = next;
                }
            }
        } else if let Some((i, next)) = c.ident() {
            id.push_str(&i.to_string());
            prev_pos = end_pos(i.span());
            c = next;
        } else {
            return Ok((id, c));
        }
    }
}

fn parse_seq(delim_ch: char, mut c: Cursor) -> Result<(Vec<Stx>, Cursor)> {
    let mut stxs = Vec::new();
    loop {
        c = skip_commas(c);

        if c.eof() {
            return Err(Error::new(c.span(), &format!("Expected {:?}", delim_ch)));
        }

        if let Some((p, next)) = c.punct() {
            if p.as_char() == delim_ch {
                return Ok((stxs, next));
            }
        }

        let (stx, next) = parse1(c)?;
        stxs.push(stx);
        c = next;
    }
}

fn skip_commas(mut c: Cursor) -> Cursor {
    loop {
        if let Some((',', next)) = punct_char(c) {
            c = next;
            continue;
        }
        return c;
    }
}

fn parse_group<'c, R, F: Fn(Cursor<'c>) -> Result<(R, Cursor<'c>)>>(
    mut c: Cursor<'c>,
    f: F,
    after: Cursor<'c>,
) -> Result<(Vec<R>, Cursor<'c>)> {
    let mut stxs = Vec::new();
    loop {
        c = skip_commas(c);
        if c.eof() {
            return Ok((stxs, after));
        }
        let (stx, next) = f(c)?;
        stxs.push(stx);
        c = next;
    }
}

fn parse_kv(c: Cursor) -> Result<((Stx, Stx), Cursor)> {
    let (k, c) = parse1(c)?;
    if let Some((':', c)) = punct_char(c) {
        let (v, c) = parse1(c)?;
        return Ok(((k, v), c));
    }
    Err(Error::new(c.span(), "Expected ':'"))
}

fn adjacent_ident(pos: LineColumn, c: Cursor) -> (Option<Ident>, Cursor) {
    if start_pos(c.span()) != pos {
        (None, c)
    } else if let Some((id, next)) = c.ident() {
        (Some(id), next)
    } else {
        (None, c)
    }
}

fn parse_exactly_one<'c>(c: Cursor<'c>) -> Result<Stx> {
    parse1(c).and_then(|(q, c)| if c.eof() {
        Ok(q)
    } else {
        Err(Error::new(c.span(), "No more input expected"))
    })
}

fn parse_generic<T: Parse>(mut c: Cursor) -> Option<(T, Cursor)> {
    match T::parse.parse2(c.token_stream()) {
        Ok(t) => Some((t, Cursor::empty())), // because parse2 checks for end-of-stream!
        Err(e) => {
            // OK, because parse2 checks for end-of-stream, let's chop
            // the input at the position of the error and try again (!).
            let mut collected = Vec::new();
            let upto = start_pos(e.span());
            while !c.eof() && start_pos(c.span()) != upto {
                let (tt, next) = c.token_tree().unwrap();
                collected.push(tt);
                c = next;
            }
            match T::parse.parse2(collected.into_iter().collect()) {
                Ok(t) => Some((t, c)),
                Err(_) => None,
            }
        }
    }
}

fn parse1(c: Cursor) -> Result<(Stx, Cursor)> {
    if let Some((p, next)) = c.punct() {
        match p.as_char() {
            '<' => parse_seq('>', next).and_then(|(mut q,c)| if q.is_empty() {
                Err(Error::new(c.span(), "Missing Record label"))
            } else {
                Ok((Stx::Rec(Box::new(q.remove(0)), q), c))
            }),
            '$' => {
                let (maybe_id, next) = adjacent_ident(end_pos(p.span()), next);
                let (maybe_type, next) = if let Some((':', next)) = punct_char(next) {
                    match parse_generic::<Type>(next) {
                        Some((t, next)) => (Some(t), next),
                        None => (None, next)
                    }
                } else {
                    (None, next)
                };
                if let Some((inner, _, next)) = next.group(Delimiter::Brace) {
                    parse_exactly_one(inner).map(
                        |q| (Stx::Binder(maybe_id, maybe_type, Some(Box::new(q))), next))
                } else {
                    Ok((Stx::Binder(maybe_id, maybe_type, None), next))
                }
            }
            '#' => {
                if let Some((inner, _, next)) = next.group(Delimiter::Brace) {
                    parse_group(inner, parse1, next).map(|(q,c)| (Stx::Set(q),c))
                } else if let Some((inner, _, next)) = next.group(Delimiter::Parenthesis) {
                    Ok((Stx::Subst(inner.token_stream()), next))
                } else if let Some((tt, next)) = next.token_tree() {
                    Ok((Stx::Subst(vec![tt].into_iter().collect()), next))
                } else {
                    Err(Error::new(c.span(), "Expected expression to substitute"))
                }
            }
            _ => Err(Error::new(c.span(), "Unexpected punctuation")),
        }
    } else if let Some((i, next)) = c.ident() {
        if i.to_string() == "_" {
            Ok((Stx::Discard, next))
        } else {
            parse_id(c).and_then(|(q,c)| Ok((Stx::Atom(IOValue::symbol(&q)), c)))
        }
    } else if let Some((literal, next)) = c.literal() {
        let t: ExprLit = syn::parse_str(&literal.to_string())?;
        let v = match t.lit {
            Lit::Str(s) => IOValue::new(s.value()),
            Lit::ByteStr(bs) => IOValue::new(&bs.value()[..]),
            Lit::Byte(b) => IOValue::new(b.value()),
            Lit::Char(_) => return Err(Error::new(c.span(), "Literal characters not supported")),
            Lit::Int(i) => if i.suffix().starts_with("u") || !i.base10_digits().starts_with("-") {
                IOValue::new(i.base10_parse::<u128>()?)
            } else {
                IOValue::new(i.base10_parse::<i128>()?)
            }
            Lit::Float(f) => if f.suffix() == "f32" {
                IOValue::new(&Double(f.base10_parse::<f32>()? as f64))
            } else {
                IOValue::new(&Double(f.base10_parse::<f64>()?))
            }
            Lit::Bool(_) => return Err(Error::new(c.span(), "Literal booleans not supported")),
            Lit::Verbatim(_) => return Err(Error::new(c.span(), "Verbatim literals not supported")),
        };
        Ok((Stx::Atom(v), next))
    } else if let Some((inner, _, after)) = c.group(Delimiter::Brace) {
        parse_group(inner, parse_kv, after).map(|(q,c)| (Stx::Dict(q),c))
    } else if let Some((inner, _, after)) = c.group(Delimiter::Bracket) {
        parse_group(inner, parse1, after).map(|(q,c)| (Stx::Seq(q),c))
    } else {
        Err(Error::new(c.span(), "Unexpected input"))
    }
}
