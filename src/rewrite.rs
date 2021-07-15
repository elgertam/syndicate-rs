use preserves::value::Map;
use preserves::value::NestedValue;
use preserves::value::Value;

use std::convert::TryFrom;

use super::schemas::sturdy::*;

pub type CheckedRewrite = (usize, Pattern, Template);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CheckedCaveat { alts: Vec<CheckedRewrite> }

#[derive(Debug)]
pub enum CaveatError {
    UnboundRef,
    BindingUnderNegation,
    LudicrousArity,
    IndexOutOfBounds,
    InvalidIndex,
    IncompleteTemplate,
}

impl Attenuation {
    pub fn validate(&self) -> Result<(), CaveatError> {
        for c in &self.0 { c.validate()? }
        Ok(())
    }

    pub fn check(&self) -> Result<Vec<CheckedCaveat>, CaveatError> {
        self.0.iter().map(Caveat::check).collect()
    }
}

impl Caveat {
    pub fn validate(&self) -> Result<(), CaveatError> {
        match self {
            Caveat::Rewrite(b) => (&**b).validate(),
            Caveat::Alts(b) => (&**b).alternatives.iter().map(Rewrite::validate).collect::<Result<(), _>>(),
        }
    }

    pub fn check(&self) -> Result<CheckedCaveat, CaveatError> {
        match self {
            Caveat::Rewrite(b) =>
                Ok(CheckedCaveat {
                    alts: vec![ (*b).check()? ]
                }),
            Caveat::Alts(b) => {
                let Alts { alternatives } = &**b;
                Ok(CheckedCaveat {
                    alts: alternatives.into_iter().map(Rewrite::check)
                        .collect::<Result<Vec<CheckedRewrite>, CaveatError>>()?
                })
            }
        }
    }
}

impl ConstructorSpec {
    fn arity(&self) -> Result<Option<usize>, CaveatError> {
        match self {
            ConstructorSpec::CRec(b) => match usize::try_from(&(&**b).arity) {
                Err(_) => Err(CaveatError::LudicrousArity),
                Ok(a) => Ok(Some(a)),
            }
            ConstructorSpec::CArr(b) => match usize::try_from(&(&**b).arity) {
                Err(_) => return Err(CaveatError::LudicrousArity),
                Ok(a) => Ok(Some(a)),
            }
            ConstructorSpec::CDict(_) => Ok(None),
        }
    }
}

fn check_member_key(limit: usize, k: &_Any) -> Result<(), CaveatError> {
    match k.value().as_signedinteger() {
        None => Err(CaveatError::InvalidIndex),
        Some(k) => match usize::try_from(k) {
            Err(_) => Err(CaveatError::IndexOutOfBounds),
            Ok(k) =>
                if k >= limit {
                    Err(CaveatError::IndexOutOfBounds)
                } else {
                    Ok(())
                },
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
            Pattern::PCompound(b) => {
                let PCompound { ctor, members: PCompoundMembers(ms) } = &**b;
                let arity = ctor.arity()?;
                let mut count = 0;
                for (k, p) in ms.iter() {
                    if let Some(limit) = arity {
                        check_member_key(limit, k)?;
                    }
                    count += p.binding_count()?
                }
                Ok(count)
            }
        }
    }

    fn matches(&self, a: &_Any, bindings: &mut Vec<_Any>) -> bool {
        match self {
            Pattern::PDiscard(_) => true,
            Pattern::PAtom(b) => match &**b {
                PAtom::Boolean => a.value().is_boolean(),
                PAtom::Float => a.value().is_float(),
                PAtom::Double => a.value().is_double(),
                PAtom::SignedInteger => a.value().is_signedinteger(),
                PAtom::String => a.value().is_string(),
                PAtom::ByteString => a.value().is_bytestring(),
                PAtom::Symbol => a.value().is_symbol(),
            }
            Pattern::PEmbedded(_) => a.value().is_embedded(),
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
                PCompound { ctor: ConstructorSpec::CRec(b), members: PCompoundMembers(ms) } => {
                    let CRec { label, arity } = &**b;
                    let arity = usize::try_from(arity).expect("in-range arity");
                    match a.value().as_record(Some(arity)) {
                        Some(r) => {
                            if r.label() != label { return false; }
                            for (k, p) in ms.iter() {
                                let k = k.value().as_signedinteger().expect("integer index");
                                let k = usize::try_from(k).expect("in-range index");
                                if !p.matches(&r.fields()[k], bindings) { return false; }
                            }
                            true
                        },
                        None => false,
                    }
                }
                PCompound { ctor: ConstructorSpec::CArr(b), members: PCompoundMembers(ms) } => {
                    let CArr { arity } = &**b;
                    let arity = usize::try_from(arity).expect("in-range arity");
                    match a.value().as_sequence() {
                        Some(vs) => {
                            if vs.len() < arity { return false; }
                            for (k, p) in ms.iter() {
                                let k = k.value().as_signedinteger().expect("integer index");
                                let k = usize::try_from(k).expect("in-range index");
                                if !p.matches(&vs[k], bindings) { return false; }
                            }
                            true
                        },
                        None => false,
                    }
                }
                PCompound { ctor: ConstructorSpec::CDict(_), members: PCompoundMembers(ms) } => {
                    match a.value().as_dictionary() {
                        Some(es) => {
                            for (k, p) in ms.iter() {
                                match es.get(k) {
                                    Some(v) => if !p.matches(v, bindings) { return false; },
                                    None => return false,
                                }
                            }
                            true
                        }
                        None => false,
                    }
                }
            }
        }
    }
}

impl Template {
    fn implied_binding_count(&self) -> Result<usize, CaveatError> {
        match self {
            Template::TAttenuate(b) => {
                let TAttenuate { template, attenuation } = &**b;
                attenuation.validate()?;
                Ok(template.implied_binding_count()?)
            }
            Template::TRef(b) => match usize::try_from(&(&**b).binding) {
                Ok(v) => Ok(1 + v),
                Err(_) => Err(CaveatError::UnboundRef),
            },
            Template::Lit(_) => Ok(0),
            Template::TCompound(b) => {
                let TCompound { ctor, members: TCompoundMembers(ms) } = &**b;
                let arity = ctor.arity()?;
                let mut max = 0;
                if let Some(limit) = arity {
                    if ms.len() != limit {
                        return Err(CaveatError::IncompleteTemplate);
                    }
                }
                for (k, t) in ms.iter() {
                    if let Some(limit) = arity {
                        check_member_key(limit, k)?;
                    }
                    max = max.max(t.implied_binding_count()?);
                }
                Ok(max)
            }
        }
    }

    fn instantiate(&self, bindings: &Vec<_Any>) -> Option<_Any> {
        match self {
            Template::TAttenuate(b) => {
                let TAttenuate { template, attenuation } = &**b;
                template.instantiate(bindings)
                    .and_then(|r| r.value().as_embedded().cloned())
                    .map(|r| Value::Embedded(r.attenuate(attenuation).expect("checked attenuation")).wrap())
            }
            Template::TRef(b) => Some(bindings[usize::try_from(&(&**b).binding).expect("in-range index")].clone()),
            Template::Lit(b) => Some((&**b).value.clone()),
            Template::TCompound(b) => {
                let TCompound { ctor, members: TCompoundMembers(ms) } = &**b;
                match ctor {
                    ConstructorSpec::CRec(b) => {
                        let CRec { label, arity } = &**b;
                        let arity = usize::try_from(arity).expect("in-range arity");
                        let mut r = Value::record(label.clone(), arity);
                        for i in 0..arity {
                            let t = ms.get(&Value::from(i).wrap()).expect("entry for each index");
                            match t.instantiate(bindings) {
                                None => return None,
                                Some(v) => r.fields_vec_mut().push(v),
                            }
                        }
                        Some(r.finish().wrap())
                    }
                    ConstructorSpec::CArr(b) => {
                        let CArr { arity } = &**b;
                        let arity = usize::try_from(arity).expect("in-range arity");
                        let mut r = Vec::with_capacity(arity);
                        for i in 0..arity {
                            let t = ms.get(&Value::from(i).wrap()).expect("entry for each index");
                            match t.instantiate(bindings) {
                                None => return None,
                                Some(v) => r.push(v),
                            }
                        }
                        Some(Value::from(r).wrap())
                    }
                    ConstructorSpec::CDict(_) => {
                        let mut r = Map::new();
                        for (k, t) in ms.iter() {
                            match t.instantiate(bindings) {
                                None => return None,
                                Some(v) => {
                                    r.insert(k.clone(), v);
                                    ()
                                }
                            }
                        }
                        Some(Value::from(r).wrap())
                    }
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
    pub fn rewrite(&self, a: &_Any) -> Option<_Any> {
        for (n, p, t) in &self.alts {
            let mut bindings = Vec::with_capacity(*n);
            if let true = p.matches(a, &mut bindings) {
                return t.instantiate(&bindings);
            }
        }
        None
    }
}
