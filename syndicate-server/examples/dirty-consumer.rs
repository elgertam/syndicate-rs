//! I am a low-level hack intended to consume bytes as quickly as
//! possible, so that the consumer isn't the bottleneck in
//! single-producer/single-consumer broker throughput measurement.

use preserves_schema::Codec;

use structopt::StructOpt;

use syndicate::schemas::Language;
use syndicate::schemas::protocol as P;
use syndicate::schemas::dataspace::Observe;
use syndicate::sturdy;
use syndicate::value::BinarySource;
use syndicate::value::BytesBinarySource;
use syndicate::value::IOValue;
use syndicate::value::PackedWriter;
use syndicate::value::Reader;

use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use std::time::Instant;

mod dirty;

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(short = "d", default_value = "b4b303726566b10973796e646963617465b584b21069ca300c1dbfa08fba692102dd82311a84")]
    dataspace: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_args();

    let mut stream = TcpStream::connect("127.0.0.1:9001")?;
    dirty::dirty_resolve(&mut stream, &config.dataspace)?;

    let iolang = Language::<IOValue>::default();

    {
        let turn = P::Turn::<IOValue>(vec![
            P::TurnEvent {
                oid: P::Oid(1.into()),
                event: P::Event::Assert(Box::new(P::Assert {
                    assertion: P::Assertion(iolang.unparse(&Observe {
                        pattern: syndicate_macros::pattern!{<Says $ $>},
                        observer: iolang.unparse(&sturdy::WireRef::Mine {
                            oid: Box::new(sturdy::Oid(2.into())),
                        }),
                    })),
                    handle: P::Handle(2.into()),
                })),
            }
        ]);
        stream.write_all(&PackedWriter::encode_iovalue(&iolang.unparse(&turn))?)?;
    }

    let mut buf = [0; 131072];
    let turn_size = {
        stream.read(&mut buf)?;
        let mut src = BytesBinarySource::new(&buf);
        src.packed_iovalues().demand_next(false)?;
        src.index
    };

    let mut start = Instant::now();
    let interval = Duration::from_secs(1);
    let mut deadline = start + interval;
    let mut total_bytes = 0;
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total_bytes += n;

        let now = Instant::now();
        if now >= deadline {
            let delta = now - start;
            let message_count = total_bytes as f64 / turn_size as f64;
            println!("{} messages in the last second ({} Hz)",
                     message_count,
                     message_count / delta.as_secs_f64());

            start = now;
            total_bytes = 0;
            deadline = deadline + interval;
        }
    }

    Ok(())
}
