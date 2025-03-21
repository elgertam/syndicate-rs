//! I am a low-level hack intended to shovel bytes out the gate as
//! quickly as possible, so that the producer isn't the bottleneck in
//! single-producer/single-consumer broker throughput measurement.

use preserves_schema::Codec;

use structopt::StructOpt;

use syndicate::schemas::Language;
use syndicate::schemas::protocol as P;
use syndicate::preserves::IOValue;
use syndicate::preserves::PackedWriter;
use syndicate::preserves::Record;
use syndicate::preserves::Value;

use std::io::Write;
use std::net::TcpStream;

mod dirty;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "a", default_value = "1")]
    action_count: u32,

    #[structopt(short = "b", default_value = "0")]
    bytes_padding: usize,

    #[structopt(short = "d", default_value = "b4b303726566b7b3036f6964b10973796e646963617465b303736967b21069ca300c1dbfa08fba692102dd82311a8484")]
    dataspace: String,
}

#[inline]
fn says(who: IOValue, what: IOValue) -> Value<IOValue> {
    Value::new(Record::_from_vec(vec![Value::symbol("Says"), who.into(), what.into()]))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args();

    let mut stream = TcpStream::connect("127.0.0.1:9001")?;
    dirty::dirty_resolve(&mut stream, &config.dataspace)?;

    let padding = IOValue::bytes(vec![0; config.bytes_padding]);
    let mut events = Vec::new();
    for _ in 0 .. config.action_count {
        events.push(P::TurnEvent::<IOValue> {
            oid: P::Oid(1.into()),
            event: P::Event::Message(Box::new(P::Message {
                body: P::Assertion(says(IOValue::new("producer"), padding.clone())),
            })),
        });
    }
    let turn = P::Turn(events);

    let mut buf: Vec<u8> = vec![];
    let iolang = Language::<IOValue>::default();
    while buf.len() < 16384 {
        buf.extend(&PackedWriter::encode_iovalue(&iolang.unparse(&turn).into())?);
    }

    loop {
        stream.write_all(&buf)?;
    }
}
