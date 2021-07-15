use getrandom::getrandom;

use hmac::{Hmac, Mac, NewMac, crypto_mac::MacError};

use preserves::hex::HexParser;
use preserves::hex::HexFormatter;
use preserves::value::Embeddable;
use preserves::value::NestedValue;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::packed::PackedWriter;
use preserves::value::packed::from_bytes;

use sha2::Sha256;

use std::convert::TryFrom;
use std::io;

use super::error::Error;
use super::rewrite::CaveatError;
pub use super::schemas::sturdy::*;

#[derive(Debug)]
pub enum ValidationError {
    SignatureError(MacError),
    AttenuationError(CaveatError),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ValidationError::SignatureError(_) =>
                write!(f, "Invalid SturdyRef signature"),
            ValidationError::AttenuationError(e) =>
                write!(f, "Invalid SturdyRef attenuation: {:?}", e),
        }
    }
}

impl std::error::Error for ValidationError {}

const KEY_LENGTH: usize = 16; // bytes; 128 bits

fn signature(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m  = Hmac::<Sha256>::new_from_slice(key).expect("valid key length");
    m.update(data);
    let mut result = m.finalize().into_bytes().to_vec();
    result.truncate(KEY_LENGTH);
    result
}

pub fn new_key() -> Vec<u8> {
    let mut buf = vec![0; KEY_LENGTH];
    getrandom(&mut buf).expect("successful random number generation");
    buf
}

pub fn encode<D: Embeddable, N: NestedValue<D>>(v: &N) -> Vec<u8> {
    PackedWriter::encode::<D, N, _>(&mut NoEmbeddedDomainCodec, v).expect("no io errors")
}

pub fn decode<D: Embeddable, N: NestedValue<D>>(bs: &[u8]) -> io::Result<N> {
    from_bytes(bs, &mut NoEmbeddedDomainCodec)
}

impl SturdyRef {
    pub fn mint(oid: _Any, key: &[u8]) -> Self {
        let sig = signature(key, &encode(&oid));
        SturdyRef { oid, caveat_chain: Vec::new(), sig }
    }

    pub fn from_hex(s: &str) -> Result<Self, Error> {
        let binary = HexParser::Liberal.decode(s).expect("hex encoded sturdyref");
        Ok(Self::try_from(&decode::<_, _Any>(&binary)?)?)
    }

    pub fn to_hex(&self) -> String {
        HexFormatter::Packed.encode(&encode::<_, _Any>(&self.into()))
    }

    pub fn validate_and_attenuate(
        &self,
        key: &[u8],
        unattenuated_target: &_Ptr,
    ) -> Result<_Ptr, ValidationError> {
        self.validate(key).map_err(ValidationError::SignatureError)?;
        let mut attenuation = Vec::new();
        // TODO:: Make sure of the ordering here!!
        for a in self.caveat_chain.iter().rev() {
            attenuation.extend(a.0.iter().rev().cloned());
        }
        let target = unattenuated_target
            .attenuate(&Attenuation(attenuation))
            .map_err(ValidationError::AttenuationError)?;
        Ok(target)
    }

    pub fn validate(&self, key: &[u8]) -> Result<(), MacError> {
        let SturdyRef { oid, caveat_chain, sig } = self;
        let mut key = key.to_vec();
        key = signature(&key, &encode(oid));
        for c in caveat_chain {
            key = signature(&key, &encode(&_Any::from(c)));
        }
        if &key == sig {
            Ok(())
        } else {
            Err(MacError)
        }
    }

    pub fn attenuate(&self, attenuation: &Attenuation) -> Result<Self, CaveatError> {
        attenuation.validate()?;
        let SturdyRef { oid, caveat_chain, sig } = self;
        let oid = oid.clone();
        let mut caveat_chain = caveat_chain.clone();
        caveat_chain.push(attenuation.clone());
        let sig = signature(&sig, &encode(&_Any::from(attenuation)));
        Ok(SturdyRef { oid, caveat_chain, sig })
    }
}
