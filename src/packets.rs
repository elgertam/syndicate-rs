use super::V;

use bytes::{Buf, buf::BufMutExt, BytesMut};
use std::sync::Arc;
use std::marker::PhantomData;

use preserves::{
    de::Deserializer,
    error,
    ser::to_writer,
    value::reader::from_bytes,
};

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

pub type Error = error::Error;

pub struct Codec<InT, OutT> {
    ph_in: PhantomData<InT>,
    ph_out: PhantomData<OutT>,
}

pub type ServerCodec = Codec<C2S, S2C>;
pub type ClientCodec = Codec<S2C, C2S>;

impl<InT, OutT> Codec<InT, OutT> {
    pub fn new() -> Self {
        Codec { ph_in: PhantomData, ph_out: PhantomData }
    }
}

impl<InT: serde::de::DeserializeOwned, OutT> tokio_util::codec::Decoder for Codec<InT, OutT> {
    type Item = InT;
    type Error = Error;
    fn decode(&mut self, bs: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut r = from_bytes(bs);
        let mut d = Deserializer::from_reader(&mut r);
        match Self::Item::deserialize(&mut d) {
            Err(e) if error::is_eof_error(&e) => Ok(None),
            Err(e) => Err(e),
            Ok(item) => {
                let count = d.read.source.index;
                bs.advance(count);
                Ok(Some(item))
            }
        }
    }
}

impl<InT, OutT: serde::Serialize> tokio_util::codec::Encoder<OutT> for Codec<InT, OutT>
{
    type Error = Error;
    fn encode(&mut self, item: OutT, bs: &mut BytesMut) -> Result<(), Self::Error> {
        to_writer(&mut bs.writer(), &item)
    }
}
