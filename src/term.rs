use std::hash::Hash;
use std::hash::Hasher;

#[derive(Debug, PartialOrd, Clone)]
pub enum Term<'a> {
    Boolean(bool),
    Float(f32),
    Double(f64),
    SignedInteger(i64), // TODO: bignums
    String(std::string::String),
    ByteString(&'a [u8]),
    Symbol(std::string::String),

    Rec(&'a Term<'a>, Vec<Term<'a>>),
    Seq(Vec<Term<'a>>),
}

impl<'a> Term<'a> {
    pub fn is_rec(&self) -> bool { match self { Term::Rec(_, _) => true, _ => false } }
    pub fn is_seq(&self) -> bool { match self { Term::Seq(_) => true, _ => false } }

    pub fn label(&self) -> &Term {
        match self {
            Term::Rec(l, _) => l,
            _ => panic!()
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Term::Seq(vs) => vs.len(),
            Term::Rec(_, fs) => fs.len(),
            _ => 0,
        }
    }
}

impl<'a> std::ops::Index<usize> for Term<'a> {
    type Output = Term<'a>;
    fn index(&self, i: usize) -> &Term<'a> {
        match self {
            Term::Seq(vs) => &vs[i],
            Term::Rec(_, fs) => &fs[i],
            _ => panic!()
        }
    }
}

impl<'a> Eq for Term<'a> {}
impl<'a> PartialEq for Term<'a> {
    fn eq(&self, other: &Term<'a>) -> bool {
        match (self, other) {
            (Term::Boolean(a), Term::Boolean(b)) => a == b,
            (Term::Float(a), Term::Float(b)) => a.to_bits() == b.to_bits(),
            (Term::Double(a), Term::Double(b)) => a.to_bits() == b.to_bits(),
            (Term::SignedInteger(a), Term::SignedInteger(b)) => a == b,
            (Term::String(a), Term::String(b)) => a == b,
            (Term::ByteString(a), Term::ByteString(b)) => a == b,
            (Term::Symbol(a), Term::Symbol(b)) => a == b,
            (Term::Seq(a), Term::Seq(b)) => a == b,
            (Term::Rec(la, fa), Term::Rec(lb, fb)) => la == lb && fa == fb,
            (_, _) => false
        }
    }
}

impl<'a> Ord for Term<'a> {
    fn cmp(&self, other: &Term<'a>) -> std::cmp::Ordering {
        match (self, other) {
            (Term::Float(a), Term::Float(b)) => {
                let mut va: i32 = a.to_bits() as i32;
                let mut vb: i32 = b.to_bits() as i32;
                if va < 0 { va ^= 0x7fffffff; }
                if vb < 0 { vb ^= 0x7fffffff; }
                va.cmp(&vb)
            }
            (Term::Double(a), Term::Double(b)) => {
                let mut va: i64 = a.to_bits() as i64;
                let mut vb: i64 = b.to_bits() as i64;
                if va < 0 { va ^= 0x7fffffffffffffff; }
                if vb < 0 { vb ^= 0x7fffffffffffffff; }
                va.cmp(&vb)
            }
            (Term::Seq(a), Term::Seq(b)) => a.cmp(b),
            (Term::Rec(la, fa), Term::Rec(lb, fb)) => {
                match la.cmp(lb) {
                    std::cmp::Ordering::Equal => fa.cmp(fb),
                    o => o
                }
            }
            (_, _) => self.partial_cmp(other).expect("total order"),
        }
    }
}

impl<'a> Hash for Term<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Term::Boolean(b) => { 1.hash(state); b.hash(state) }
            Term::Float(f) => { 2.hash(state); f.to_bits().hash(state) }
            Term::Double(d) => { 3.hash(state); d.to_bits().hash(state) }
            Term::SignedInteger(i) => { 4.hash(state); i.hash(state) }
            Term::String(s) => { 5.hash(state); s.hash(state) }
            Term::ByteString(b) => { 6.hash(state); b.hash(state) }
            Term::Symbol(s) => { 7.hash(state); s.hash(state) }
            Term::Rec(l, fs) => { 8.hash(state); l.hash(state); fs.hash(state) }
            Term::Seq(vs) => { 9.hash(state); vs.hash(state) }
        }
    }
}
