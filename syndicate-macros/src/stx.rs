use proc_macro2::Delimiter;
use proc_macro2::LineColumn;
use proc_macro2::TokenTree;

use syn::ExprLit;
use syn::Lit;
use syn::Result;
use syn::buffer::Cursor;
use syn::parse::Error;
use syn::parse::Parse;
use syn::parse::ParseStream;

use syndicate::value::Float;
use syndicate::value::Double;
use syndicate::value::IOValue;
use syndicate::value::NestedValue;

#[derive(Debug, Clone)]
pub enum Stx {
    Atom(IOValue),
    Binder(Option<String>, Box<Stx>),
    Discard,
    Subst(TokenTree),
    Ctor1(String, Box<Stx>),
    CtorN(String, Vec<(Stx, Stx)>),
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

fn parse_id(mut c: Cursor) -> Result<(String, Cursor)> {
    let mut id = String::new();
    let mut prev_pos = c.span().start();
    loop {
        if c.eof() || c.span().start() != prev_pos {
            return Ok((id, c));
        } else if let Some((p, next)) = c.punct() {
            match p.as_char() {
                '<' | '>' | '(' | ')' | '{' | '}' | '[' | ']' | ',' | ':' => return Ok((id, c)),
                ch => {
                    id.push(ch);
                    prev_pos = c.span().end();
                    c = next;
                }
            }
        } else if let Some((i, next)) = c.ident() {
            id.push_str(&i.to_string());
            prev_pos = i.span().end();
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
        if let Some((p, next)) = c.punct() {
            if p.as_char() == ',' {
                c = next;
                continue;
            }
        }
        return c;
    }
}

fn parse_group_inner<'c, R, F: Fn(Cursor<'c>) -> Result<(R, Cursor<'c>)>>(
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

fn parse_group<'c, R, F: Fn(Cursor<'c>) -> Result<(R, Cursor<'c>)>>(
    d: Delimiter,
    f: F,
    c: Cursor<'c>,
) -> Result<(Vec<R>, Cursor<'c>)> {
    let (inner, _, after) = c.group(d).unwrap();
    parse_group_inner(inner, f, after)
}

fn parse_kv(c: Cursor) -> Result<((Stx, Stx), Cursor)> {
    let (k, c) = parse1(c)?;
    if let Some((p, c)) = c.punct() {
        if p.as_char() == ':' {
            let (v, c) = parse1(c)?;
            return Ok(((k, v), c));
        }
    }
    Err(Error::new(c.span(), "Expected ':'"))
}

fn adjacent_id(pos: LineColumn, c: Cursor) -> (Option<String>, Cursor) {
    if c.span().start() != pos {
        (None, c)
    } else {
        let (s, next) = parse_id(c).unwrap();
        if s.is_empty() {
            (None, c)
        } else {
            (Some(s), next)
        }
    }
}

fn parse_exactly_one<'c>(c: Cursor<'c>) -> Result<Stx> {
    parse1(c).and_then(|(q, c)| if c.eof() {
        Ok(q)
    } else {
        Err(Error::new(c.span(), "No more input expected"))
    })
}

fn parse1(c: Cursor) -> Result<(Stx, Cursor)> {
    if let Some((p, next)) = c.punct() {
        match p.as_char() {
            '<' => parse_seq('>', next).and_then(|(mut q,c)| if q.is_empty() {
                Err(Error::new(c.span(), "Missing Record label"))
            } else {
                Ok((Stx::Rec(Box::new(q.remove(0)), q), c))
            }),
            '{' => parse_group(Delimiter::Brace, parse_kv, c).map(|(q,c)| (Stx::Dict(q),c)),
            '[' => parse_group(Delimiter::Bracket, parse1, c).map(|(q,c)| (Stx::Seq(q),c)),
            '$' => {
                let (maybe_id, next) = adjacent_id(p.span().end(), next);
                if let Some((inner, _, next)) = next.group(Delimiter::Parenthesis) {
                    parse_exactly_one(inner).map(
                        |q| (Stx::Binder(maybe_id, Box::new(q)), next))
                } else {
                    Ok((Stx::Binder(maybe_id, Box::new(Stx::Discard)), next))
                }
            }
            '#' => {
                if let Some((inner, _, next)) = next.group(Delimiter::Brace) {
                    parse_group_inner(inner, parse1, next).map(|(q,c)| (Stx::Set(q),c))
                } else if let Some((tt, next)) = next.token_tree() {
                    Ok((Stx::Subst(tt), next))
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
            parse_id(c).and_then(|(q,c)| {
                if let Some((inner, _, next)) = c.group(Delimiter::Parenthesis) {
                    match parse_group_inner(inner, parse_kv, next) {
                        Ok((kvs, next)) => Ok((Stx::CtorN(q, kvs), next)),
                        Err(_) => parse_exactly_one(inner).map(
                            |v| (Stx::Ctor1(q, Box::new(v)), next)),
                    }
                } else {
                    Ok((Stx::Atom(IOValue::symbol(&q)), c))
                }
            })
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
                IOValue::new(&Float(f.base10_parse::<f32>()?))
            } else {
                IOValue::new(&Double(f.base10_parse::<f64>()?))
            }
            Lit::Bool(_) => return Err(Error::new(c.span(), "Literal booleans not supported")),
            Lit::Verbatim(_) => return Err(Error::new(c.span(), "Verbatim literals not supported")),
        };
        Ok((Stx::Atom(v), next))
    } else {
        Err(Error::new(c.span(), "Unexpected input"))
    }
}
