use crate::schemas::dataspace_patterns::*;

use preserves::value::NestedValue;
use preserves::value::Value;

pub fn lift_literal<N: NestedValue>(v: &N) -> Pattern<N> {
    match v.value() {
        Value::Boolean(b) => Pattern::Lit { value: Box::new(AnyAtom::Bool(*b)) },
        Value::Double(d) => Pattern::Lit { value: Box::new(AnyAtom::Double(*d)) },
        Value::SignedInteger(i) => Pattern::Lit { value: Box::new(AnyAtom::Int(i.clone())) },
        Value::String(s) => Pattern::Lit { value: Box::new(AnyAtom::String(s.clone())) },
        Value::ByteString(bs) => Pattern::Lit { value: Box::new(AnyAtom::Bytes(bs.clone())) },
        Value::Symbol(s) => Pattern::Lit { value: Box::new(AnyAtom::Symbol(s.clone())) },
        Value::Record(r) => Pattern::Group {
            type_: Box::new(GroupType::Rec { label: r.label().clone() }),
            entries: r.fields().iter().enumerate()
                .map(|(i, v)| (N::new(i), lift_literal(v)))
                .collect(),
        },
        Value::Sequence(items) => Pattern::Group {
            type_: Box::new(GroupType::Arr),
            entries: items.iter().enumerate()
                .map(|(i, v)| (N::new(i), lift_literal(v)))
                .collect(),
        },
        Value::Set(_members) => panic!("Cannot express literal set in pattern"),
        Value::Dictionary(entries) => Pattern::Group {
            type_: Box::new(GroupType::Dict),
            entries: entries.iter()
                .map(|(k, v)| (k.clone(), lift_literal(v)))
                .collect(),
        },
        Value::Embedded(e) => Pattern::Lit { value: Box::new(AnyAtom::Embedded(e.clone())) },
    }
}
