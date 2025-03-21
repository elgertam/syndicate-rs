//! The implementation of [capability attenuation][crate::actor::Cap]:
//! filtering and rewriting of assertions and messages.

use preserves::AtomClass;
use preserves::CompoundClass;
use preserves::Map;
use preserves::Record;
use preserves::Value;
use preserves::ValueClass;

use std::convert::TryFrom;

use super::schemas::sturdy::*;

/// A triple of (1) the count of bindings captured by (2) a checked
/// `Pattern`, plus (3) a checked `Template`.
pub type CheckedRewrite = (usize, Pattern, Template);

/// A safety-checked [`Caveat`]: none of the errors enumerated in
/// `CaveatError` apply.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CheckedCaveat {
    Alts(Vec<CheckedRewrite>),
    Reject(Pattern),
}

/// Represents any detected error in a [`Caveat`]; that is, in a
/// [`Pattern`] or a [`Template`].
#[derive(Debug)]
pub enum CaveatError {
    /// A template refers to a binding not present in the corresponding pattern.
    UnboundRef,
    /// A pattern includes a negation of a subpattern that includes a binding.
    BindingUnderNegation,
}

impl Caveat {
    /// Yields `Ok(())` iff `caveats` have no [`CaveatError`].
    pub fn validate_many(caveats: &[Caveat]) -> Result<(), CaveatError> {
        for c in caveats { c.validate()? }
        Ok(())
    }

    /// Yields `Ok(())` iff `self` has no [`CaveatError`].
    pub fn validate(&self) -> Result<(), CaveatError> {
        match self {
            Caveat::Rewrite(b) => (&**b).validate(),
            Caveat::Alts(b) => (&**b).alternatives.iter().map(Rewrite::validate).collect::<Result<(), _>>(),
            Caveat::Reject(_) => Ok(()),
            Caveat::Unknown(_) => Ok(()), /* it's valid to have unknown caveats, they just won't pass anything */
        }
    }

    /// Yields a vector of [`CheckedCaveat`s][CheckedCaveat] iff
    /// `caveats` have no [`CaveatError`].
    pub fn check_many(caveats: &[Caveat]) -> Result<Vec<CheckedCaveat>, CaveatError> {
        caveats.iter().map(Caveat::check).collect()
    }

    /// Yields a [`CheckedCaveat`] iff `self` has no [`CaveatError`].
    pub fn check(&self) -> Result<CheckedCaveat, CaveatError> {
        match self {
            Caveat::Rewrite(b) =>
                Ok(CheckedCaveat::Alts(vec![ (*b).check()? ])),
            Caveat::Alts(b) => {
                let Alts { alternatives } = &**b;
                Ok(CheckedCaveat::Alts(
                    alternatives.into_iter().map(Rewrite::check)
                        .collect::<Result<Vec<CheckedRewrite>, CaveatError>>()?))
            }
            Caveat::Reject(b) =>
                Ok(CheckedCaveat::Reject(b.pattern.clone())),
            Caveat::Unknown(_) =>
                Ok(CheckedCaveat::Reject(Pattern::PDiscard(Box::new(PDiscard)))),
        }
    }
}

impl Pattern {
    fn binding_count(&self) -> Result<usize, CaveatError> {
        match self {
            Pattern::PDiscard(_) |
            Pattern::PAtom(_) |
            Pattern::PEmbedded(_) |
            Pattern::Lit(_) => Ok(0),
            Pattern::PBind(b) => Ok(1 + (&**b).pattern.binding_count()?),
            Pattern::PNot(b) => {
                let PNot { pattern } = &**b;
                if pattern.binding_count()? == 0 {
                    Ok(0)
                } else {
                    Err(CaveatError::BindingUnderNegation)
                }
            }
            Pattern::PAnd(b) => {
                let mut count = 0;
                for p in &(&**b).patterns {
                    count += p.binding_count()?
                }
                Ok(count)
            }
            Pattern::PCompound(b) => match &**b {
                PCompound::Rec { fields: items, .. } |
                PCompound::Arr { items } => {
                    let mut count = 0;
                    for p in items.iter() {
                        count += p.binding_count()?;
                    }
                    Ok(count)
                }
                PCompound::Dict { entries } => {
                    let mut count = 0;
                    for (_k, p) in entries.iter() {
                        count += p.binding_count()?;
                    }
                    Ok(count)
                }
            }
        }
    }

    fn matches(&self, a: &_Any, bindings: &mut Vec<_Any>) -> bool {
        let ac = a.value_class();
        match self {
            Pattern::PDiscard(_) => true,
            Pattern::PAtom(b) => match &**b {
                PAtom::Boolean => ac == ValueClass::Atomic(AtomClass::Boolean),
                PAtom::Double => ac == ValueClass::Atomic(AtomClass::Double),
                PAtom::SignedInteger => ac == ValueClass::Atomic(AtomClass::SignedInteger),
                PAtom::String => ac == ValueClass::Atomic(AtomClass::String),
                PAtom::ByteString => ac == ValueClass::Atomic(AtomClass::ByteString),
                PAtom::Symbol => ac == ValueClass::Atomic(AtomClass::Symbol),
            }
            Pattern::PEmbedded(_) => ac == ValueClass::Embedded,
            Pattern::PBind(b) => {
                bindings.push(a.clone());
                (&**b).pattern.matches(a, bindings)
            }
            Pattern::PAnd(b) => {
                for p in &(&**b).patterns {
                    if !p.matches(a, bindings) { return false; }
                }
                true
            },
            Pattern::PNot(b) => !(&**b).pattern.matches(a, bindings),
            Pattern::Lit(b) => &(&**b).value == a,
            Pattern::PCompound(b) => match &**b {
                PCompound::Rec { label, fields } =>
                    ac == ValueClass::Compound(CompoundClass::Record)
                        && &a.label() == label
                        && a.len() == fields.len()
                        && {
                            for (i, v) in a.iter().enumerate() {
                                let p = &fields[i];
                                if !p.matches(&v, bindings) { return false; }
                            }
                            true
                        },
                PCompound::Arr { items } =>
                    ac == ValueClass::Compound(CompoundClass::Sequence)
                        && a.len() == items.len()
                        && {
                            for (i, v) in a.iter().enumerate() {
                                let p = &items[i];
                                if !p.matches(&v, bindings) { return false; }
                            }
                            true
                        },
                PCompound::Dict { entries } =>
                    ac == ValueClass::Compound(CompoundClass::Dictionary)
                        && {
                            for (k, p) in entries.iter() {
                                match a.get(k) {
                                    Some(v) => if !p.matches(&v, bindings) { return false; },
                                    None => return false,
                                }
                            }
                            true
                        },
            }
        }
    }
}

impl Template {
    fn implied_binding_count(&self) -> Result<usize, CaveatError> {
        match self {
            Template::TAttenuate(b) => {
                let TAttenuate { template, attenuation } = &**b;
                Caveat::validate_many(attenuation)?;
                Ok(template.implied_binding_count()?)
            }
            Template::TRef(b) => match usize::try_from(&(&**b).binding) {
                Ok(v) => Ok(1 + v),
                Err(_) => Err(CaveatError::UnboundRef),
            },
            Template::Lit(_) => Ok(0),
            Template::TCompound(b) => match &**b {
                TCompound::Rec { fields: items, .. } |
                TCompound::Arr { items } => {
                    let mut max = 0;
                    for t in items.iter() {
                        max = max.max(t.implied_binding_count()?);
                    }
                    Ok(max)
                }
                TCompound::Dict { entries } => {
                    let mut max = 0;
                    for (_k, t) in entries.iter() {
                        max = max.max(t.implied_binding_count()?);
                    }
                    Ok(max)
                }
            }
        }
    }

    fn instantiate(&self, bindings: &Vec<_Any>) -> Option<_Any> {
        match self {
            Template::TAttenuate(b) => {
                let TAttenuate { template, attenuation } = &**b;
                template.instantiate(bindings)
                    .and_then(|r| r.as_embedded().map(|c| c.into_owned()))
                    .map(|r| Value::embedded(r.attenuate(attenuation).expect("checked attenuation")))
            }
            Template::TRef(b) => Some(bindings[usize::try_from(&(&**b).binding).expect("in-range index")].clone()),
            Template::Lit(b) => Some((&**b).value.clone()),
            Template::TCompound(b) => match &**b {
                TCompound::Rec { label, fields } => {
                    let mut vs = Vec::with_capacity(fields.len() + 1);
                    vs.push(label.clone());
                    for t in fields.iter() {
                        vs.push(t.instantiate(bindings)?);
                    }
                    Some(Value::new(Record::_from_vec(vs)))
                }
                TCompound::Arr { items } => {
                    let mut r = Vec::with_capacity(items.len());
                    for t in items.iter() {
                        r.push(t.instantiate(bindings)?);
                    }
                    Some(Value::new(r))
                }
                TCompound::Dict { entries } => {
                    let mut r = Map::new();
                    for (k, t) in entries.iter() {
                        r.insert(k.clone(), t.instantiate(bindings)?);
                    }
                    Some(Value::new(r))
                }
            }
        }
    }
}

impl Rewrite {
    fn validated_binding_count(&self) -> Result<usize, CaveatError> {
        let binding_count = self.pattern.binding_count()?;
        let implied_binding_count = self.template.implied_binding_count()?;
        if implied_binding_count > binding_count { return Err(CaveatError::UnboundRef); }
        Ok(binding_count)
    }

    fn validate(&self) -> Result<(), CaveatError> {
        let _ = self.validated_binding_count()?;
        Ok(())
    }

    fn check(&self) -> Result<CheckedRewrite, CaveatError> {
        Ok((self.validated_binding_count()?, self.pattern.clone(), self.template.clone()))
    }
}

impl CheckedCaveat {
    /// Rewrites `a` using the patterns/templates contained in `self`.
    pub fn rewrite(&self, a: &_Any) -> Option<_Any> {
        match self {
            CheckedCaveat::Alts(alts) => {
                for (n, p, t) in alts {
                    let mut bindings = Vec::with_capacity(*n);
                    if p.matches(a, &mut bindings) {
                        return t.instantiate(&bindings);
                    }
                }
                None
            },
            CheckedCaveat::Reject(pat) => {
                let mut bindings = Vec::with_capacity(0);
                if pat.matches(a, &mut bindings) {
                    None
                } else {
                    Some(a.clone())
                }
            }
        }
    }
}
