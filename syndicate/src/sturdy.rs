use blake2::Blake2s256;
use getrandom::getrandom;
use hmac::{SimpleHmac, Mac};

use preserves::hex::HexParser;
use preserves::hex::HexFormatter;
use preserves::value::NestedValue;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::packed::PackedWriter;
use preserves::value::packed::from_bytes;
use preserves_schema::Codec;

use std::io;

use super::language;
use super::error::Error;
use super::rewrite::CaveatError;
pub use super::schemas::sturdy::*;

#[derive(Debug)]
pub enum ValidationError {
    SignatureError,
    AttenuationError(CaveatError),
    BadCaveatsField,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ValidationError::SignatureError =>
                write!(f, "Invalid SturdyRef signature"),
            ValidationError::AttenuationError(e) =>
                write!(f, "Invalid SturdyRef attenuation: {:?}", e),
            ValidationError::BadCaveatsField =>
                write!(f, "Invalid caveats field in SturdyRef parameters"),
        }
    }
}

impl std::error::Error for ValidationError {}

const KEY_LENGTH: usize = 16; // bytes; 128 bits

fn signature(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m  = SimpleHmac::<Blake2s256>::new_from_slice(key).expect("valid key length");
    m.update(data);
    let mut result = m.finalize().into_bytes().to_vec();
    result.truncate(KEY_LENGTH);
    result
}

fn chain_signature(key: &[u8], chain: &[Caveat]) -> Vec<u8> {
    let mut key = key.to_vec();
    for c in chain {
        key = signature(&key, &encode(&language().unparse(c)));
    }
    key
}

pub fn new_key() -> Vec<u8> {
    let mut buf = vec![0; KEY_LENGTH];
    getrandom(&mut buf).expect("successful random number generation");
    buf
}

pub fn encode<N: NestedValue>(v: &N) -> Vec<u8> {
    PackedWriter::encode(&mut NoEmbeddedDomainCodec, v).expect("no io errors")
}

pub fn decode<N: NestedValue>(bs: &[u8]) -> io::Result<N> {
    from_bytes(bs, &mut NoEmbeddedDomainCodec)
}

impl SturdyRef {
    pub fn mint(oid: _Any, key: &[u8]) -> Self {
        let sig = signature(key, &encode(&oid));
        SturdyRef::from_parts(oid, vec![], sig)
    }

    pub fn from_parts(oid: _Any, caveats: Vec<Caveat>, sig: Vec<u8>) -> Self {
        SturdyRef {
            parameters: Parameters {
                oid,
                sig,
                caveats: if caveats.is_empty() {
                    CaveatsField::Absent
                } else {
                    CaveatsField::Present { caveats }
                }
            }
        }
    }

    pub fn from_hex(s: &str) -> Result<Self, Error> {
        let binary = HexParser::Liberal.decode(s).expect("hex encoded sturdyref");
        Ok(language().parse(&decode(&binary)?)?)
    }

    pub fn to_hex(&self) -> String {
        HexFormatter::Packed.encode(&encode(&language().unparse(self)))
    }

    pub fn caveat_chain(&self) -> Result<&[Caveat], ValidationError> {
        match &self.parameters.caveats {
            CaveatsField::Absent => Ok(&[]),
            CaveatsField::Invalid { .. } => Err(ValidationError::BadCaveatsField),
            CaveatsField::Present { caveats } => Ok(caveats),
        }
    }

    pub fn validate_and_attenuate(
        &self,
        key: &[u8],
        unattenuated_target: &_Ptr,
    ) -> Result<_Ptr, ValidationError> {
        self.validate(key).map_err(|_| ValidationError::SignatureError)?;
        let target = unattenuated_target
            .attenuate(self.caveat_chain()?)
            .map_err(ValidationError::AttenuationError)?;
        Ok(target)
    }

    pub fn validate(&self, key: &[u8]) -> Result<(), ()> {
        let SturdyRef { parameters: Parameters { oid, sig, .. } } = self;
        let key = chain_signature(&signature(&key, &encode(oid)),
                                  self.caveat_chain().map_err(|_| ())?);
        if &key == sig {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn attenuate(&self, attenuation: &[Caveat]) -> Result<Self, ValidationError> {
        Caveat::validate_many(attenuation).map_err(ValidationError::AttenuationError)?;
        let SturdyRef { parameters: Parameters { oid, sig, .. } } = self;
        let oid = oid.clone();
        let mut caveat_chain = self.caveat_chain()?.to_vec();
        caveat_chain.extend(attenuation.iter().cloned());
        let sig = chain_signature(&sig, attenuation);
        Ok(SturdyRef::from_parts(oid, caveat_chain, sig))
    }
}
