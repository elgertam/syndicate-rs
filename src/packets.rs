use super::V;

use bytes::BytesMut;
use preserves::value;
use std::io;

pub type EndpointName = V;
pub type Assertion = V;
pub type Captures = Vec<Assertion>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum In {
    Connect(V),
    Turn(Vec<Action>),
    Ping(),
    Pong(),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Out {
    Err(String),
    Turn(Vec<Event>),
    Ping(),
    Pong(),
}

//---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DecodeError {
    Read(value::decoder::Error),
    Parse(value::error::Error, V),
}

impl From<io::Error> for DecodeError {
    fn from(v: io::Error) -> Self {
        DecodeError::Read(v.into())
    }
}

//---------------------------------------------------------------------------

#[derive(Debug)]
pub enum EncodeError {
    Write(value::encoder::Error),
    Unparse(value::error::Error),
}

impl From<io::Error> for EncodeError {
    fn from(v: io::Error) -> Self {
        EncodeError::Write(v)
    }
}

impl From<value::error::Error> for EncodeError {
    fn from(v: value::error::Error) -> Self {
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

//---------------------------------------------------------------------------

pub struct Codec {
    codec: value::Codec<V>,
}

impl Codec {
    pub fn new(codec: value::Codec<V>) -> Self {
        Codec { codec }
    }
}

impl tokio::codec::Decoder for Codec {
    type Item = In;
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

impl tokio::codec::Encoder for Codec {
    type Item = Out;
    type Error = EncodeError;
    fn encode(&mut self, item: Self::Item, bs: &mut BytesMut) -> Result<(), Self::Error> {
        let v: V = value::to_value(&item)?;
        bs.extend(self.codec.encode_bytes(&v)?);
        Ok(())
    }
}
