use preserves_schema::Codec;

use syndicate::schemas::Language;
use syndicate::schemas::gatekeeper;
use syndicate::schemas::protocol as P;
use syndicate::sturdy;
use syndicate::value::IOValue;
use syndicate::value::NestedValue;
use syndicate::value::PackedWriter;

use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

pub fn dirty_resolve(stream: &mut TcpStream, dataspace: &str) -> Result<(), Box<dyn std::error::Error>> {
    let iolang = Language::<IOValue>::default();

    let sturdyref = sturdy::SturdyRef::from_hex(dataspace)?;
    let sturdyref = iolang.parse::<gatekeeper::Step<IOValue>>(
        &syndicate::language().unparse(&sturdyref)
            .copy_via(&mut |_| Err("no!"))?)?;

    let resolve_turn = P::Turn(vec![
        P::TurnEvent {
            oid: P::Oid(0.into()),
            event: P::Event::Assert(Box::new(P::Assert {
                assertion: P::Assertion(iolang.unparse(&gatekeeper::Resolve::<IOValue> {
                    step: sturdyref,
                    observer: iolang.unparse(&sturdy::WireRef::Mine {
                        oid: Box::new(sturdy::Oid(0.into())),
                    }),
                })),
                handle: P::Handle(1.into()),
            })),
        }
    ]);
    stream.write_all(&PackedWriter::encode_iovalue(&iolang.unparse(&resolve_turn))?)?;

    {
        let mut buf = [0; 1024];
        stream.read(&mut buf)?;
        // We just assume we got a positive response here!!
        // We further assume that the resolved dataspace was assigned peer-oid 1
    }

    Ok(())
}
