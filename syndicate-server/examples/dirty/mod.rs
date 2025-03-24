use syndicate::schemas::gatekeeper;
use syndicate::schemas::protocol as P;
use syndicate::sturdy;
use syndicate::preserves::IOValue;
use syndicate::preserves::PackedWriter;
use syndicate::preserves::value_map_embedded;

use preserves_schema::Parse;
use preserves_schema::Unparse;

use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

pub fn dirty_resolve(stream: &mut TcpStream, dataspace: &str) -> Result<(), Box<dyn std::error::Error>> {
    let sturdyref = sturdy::SturdyRef::from_hex(dataspace)?;
    let sturdyref = gatekeeper::Step::<IOValue>::parse(
        &value_map_embedded(&sturdyref.unparse(), &mut |_| Err("no!"))?)?;

    let resolve_turn = P::Turn(vec![
        P::TurnEvent {
            oid: P::Oid(0.into()),
            event: P::Event::Assert(P::Assert {
                assertion: P::Assertion((gatekeeper::Resolve::<IOValue> {
                    step: sturdyref,
                    observer: (sturdy::WireRef::Mine {
                        oid: sturdy::Oid(0.into()),
                    }).unparse().into(),
                }).unparse()),
                handle: P::Handle(1.into()),
            }),
        }
    ]);
    stream.write_all(&PackedWriter::encode_iovalue(&resolve_turn.unparse().into())?)?;

    {
        let mut buf = [0; 1024];
        stream.read(&mut buf)?;
        // We just assume we got a positive response here!!
        // We further assume that the resolved dataspace was assigned peer-oid 1
    }

    Ok(())
}
