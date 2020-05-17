use super::V;
use super::Syndicate;

use bytes::{Buf, BytesMut};
use preserves::value;
use std::io;
use std::sync::Arc;
use std::marker::PhantomData;

pub type EndpointName = V;
pub type Assertion = V;
pub type Captures = Arc<Vec<Assertion>>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Action {
    Assert(EndpointName, Assertion),
    Clear(EndpointName),
    Message(Assertion),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Event {
    Add(EndpointName, Captures),
    Del(EndpointName, Captures),
    Msg(EndpointName, Captures),
    End(EndpointName),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum C2S {
    Connect(V),
    Turn(Vec<Action>),
    Ping(),
    Pong(),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum S2C {
    Err(String, V),
    Turn(Vec<Event>),
    Ping(),
    Pong(),
}

//---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DecodeError {
    Read(value::decoder::Error),
    Parse(value::error::Error<Syndicate>, V),
}

impl From<value::decoder::Error> for DecodeError {
    fn from(v: value::decoder::Error) -> Self {
        DecodeError::Read(v)
    }
}

impl From<io::Error> for DecodeError {
    fn from(v: io::Error) -> Self {
        DecodeError::Read(v.into())
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for DecodeError {
}

//---------------------------------------------------------------------------

#[derive(Debug)]
pub enum EncodeError {
    Write(value::encoder::Error),
    Unparse(value::error::Error<Syndicate>),
}

impl From<io::Error> for EncodeError {
    fn from(v: io::Error) -> Self {
        EncodeError::Write(v)
    }
}

impl From<value::error::Error<Syndicate>> for EncodeError {
    fn from(v: value::error::Error<Syndicate>) -> Self {
        EncodeError::Unparse(v)
    }
}

impl From<EncodeError> for io::Error {
    fn from(v: EncodeError) -> Self {
        match v {
            EncodeError::Write(e) => e,
            EncodeError::Unparse(e) =>
                Self::new(io::ErrorKind::InvalidData, format!("{:?}", e)),
        }
    }
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for EncodeError {
}
//---------------------------------------------------------------------------

pub struct Codec<InT, OutT> {
    codec: value::Codec<V, Syndicate>,
    ph_in: PhantomData<InT>,
    ph_out: PhantomData<OutT>,
}

pub type ServerCodec = Codec<C2S, S2C>;
pub type ClientCodec = Codec<S2C, C2S>;

pub fn standard_preserves_codec() -> value::Codec<V, Syndicate> {
    value::Codec::new({
        let mut m = value::Map::new();
        m.insert(0, value::Value::symbol("Discard"));
        m.insert(1, value::Value::symbol("Capture"));
        m.insert(2, value::Value::symbol("Observe"));
        m
    })
}

impl<InT, OutT> Codec<InT, OutT> {
    pub fn new(codec: value::Codec<V, Syndicate>) -> Self {
        Codec { codec, ph_in: PhantomData, ph_out: PhantomData }
    }

    pub fn standard() -> Self {
        Self::new(standard_preserves_codec())
    }
}

impl<InT: serde::de::DeserializeOwned, OutT> tokio_util::codec::Decoder for Codec<InT, OutT> {
    type Item = InT;
    type Error = DecodeError;
    fn decode(&mut self, bs: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut buf = &bs[..];
        let orig_len = buf.len();
        let res = self.codec.decode(&mut buf);
        let final_len = buf.len();
        match res {
            Ok(v) => {
                bs.advance(orig_len - final_len);
                match value::from_value(&v) {
                    Ok(p) => Ok(Some(p)),
                    Err(e) => Err(DecodeError::Parse(e, v))
                }
            }
            Err(value::decoder::Error::Eof) => Ok(None),
            Err(e) => Err(DecodeError::Read(e)),
        }
    }
}

impl<InT, OutT: serde::Serialize> tokio_util::codec::Encoder<OutT> for Codec<InT, OutT>
{
    type Error = EncodeError;
    fn encode(&mut self, item: OutT, bs: &mut BytesMut) -> Result<(), Self::Error> {
        let v: V = value::to_value(&item)?;
        bs.extend(self.codec.encode_bytes(&v)?);
        Ok(())
    }
}
