pub use crate::schemas::internal_protocol::*;

use bytes::{Buf, BufMut, BytesMut};

use std::convert::TryFrom;

use preserves::value::PackedReader;
use preserves::value::PackedWriter;
use preserves::value::Reader;
use preserves::value::Writer;

pub struct Codec;

impl tokio_util::codec::Decoder for Codec {
    type Item = Packet;
    type Error = Error;
    fn decode(&mut self, bs: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut r = PackedReader::decode_bytes(bs);
        match r.next(false)? {
            None => Ok(None),
            Some(item) => {
                let count = r.source.index;
                bs.advance(count);
                Ok(Some(Packet::try_from(&item)?))
            }
        }
    }
}

impl tokio_util::codec::Encoder<&Packet> for Codec {
    type Error = Error;
    fn encode(&mut self, item: &Packet, bs: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(PackedWriter::new(&mut bs.writer()).write(&item.into())?)
    }
}
