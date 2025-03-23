use preserves::Atom;
use preserves::CompoundClass;
use preserves::Domain;
use preserves::Double;
use preserves::Value;
use preserves::ValueClass;

use crate::schemas::dataspace_patterns::*;

pub fn lift_literal<D: Domain>(v: &Value<D>) -> Pattern<D> {
    match v.value_class() {
        ValueClass::Atomic(_) => match v.as_atom().unwrap() {
            Atom::Boolean(b) => Pattern::Lit { value: AnyAtom::Bool(b) },
            Atom::Double(d) => Pattern::Lit { value: AnyAtom::Double(Double(d)) },
            Atom::SignedInteger(i) => Pattern::Lit { value: AnyAtom::Int(i.into_owned()) },
            Atom::String(s) => Pattern::Lit { value: AnyAtom::String(s.into_owned()) },
            Atom::ByteString(bs) => Pattern::Lit { value: AnyAtom::Bytes(bs.into_owned()) },
            Atom::Symbol(s) => Pattern::Lit { value: AnyAtom::Symbol(s.into_owned()) },
        },
        ValueClass::Compound(CompoundClass::Record) => Pattern::Group {
            type_: GroupType::Rec { label: v.label().clone() },
            entries: v.iter().enumerate()
                .map(|(i, v)| (Value::new(i), lift_literal(&v)))
                .collect(),
        },
        ValueClass::Compound(CompoundClass::Sequence) => Pattern::Group {
            type_: GroupType::Arr,
            entries: v.iter().enumerate()
                .map(|(i, v)| (Value::new(i), lift_literal(&v)))
                .collect(),
        },
        ValueClass::Compound(CompoundClass::Set) => panic!("Cannot express literal set in pattern"),
        ValueClass::Compound(CompoundClass::Dictionary) => Pattern::Group {
            type_: GroupType::Dict,
            entries: v.entries()
                .map(|(k, v)| (k.clone(), lift_literal(&v)))
                .collect(),
        },
        ValueClass::Embedded => Pattern::Lit {
            value: AnyAtom::Embedded(v.as_embedded().unwrap().into_owned())
        },
    }
}
