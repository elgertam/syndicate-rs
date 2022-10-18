use std::io;

use clap::arg;
use clap::value_parser;
use clap::ArgGroup;
use clap::Command;
use clap::Id;
use clap_complete::{generate, Shell};
use preserves::hex::HexParser;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::ViaCodec;
use preserves::value::TextWriter;
use syndicate::language;
use syndicate::preserves_schema::Codec;

fn cli() -> Command {
    Command::new("syndicate-macaroon")
        .subcommand_required(true)
        .subcommand(
            Command::new("mint")
                .about("Mint a fresh macaroon")
                .arg(arg!(--oid <value> "Preserves value to use as SturdyRef OID").required(true))
                .arg(arg!(--phrase <text> "Key phrase"))
                .arg(arg!(--hex <hex> "Key bytes, encoded as hex"))
                .group(
                    ArgGroup::new("key")
                        .args(["phrase", "hex"])
                        .required(true)))
        .subcommand(
            Command::new("completions")
                .about("Generate shell completions for this command")
                .arg(arg!(<shell> "Shell dialect to generate").value_parser(value_parser!(Shell))))
}

fn main() -> io::Result<()> {
    let args = cli().get_matches();

    if let Some(args) = args.subcommand_matches("completions") {
        let shell = args.get_one::<Shell>("shell").unwrap().clone();
        let mut cmd = cli();
        let name = cmd.get_name().to_string();
        generate(shell, &mut cmd, name, &mut io::stdout());
    }

    if let Some(args) = args.subcommand_matches("mint") {
        let oid_str = args.get_one::<String>("oid").unwrap();
        let oid = match preserves::value::text::from_str(&oid_str, ViaCodec::new(NoEmbeddedDomainCodec)) {
            Ok(oid) => oid,
            Err(e) => {
                eprintln!("Could not parse oid: {}", e);
                std::process::exit(1);
            }
        };
        let key = match args.get_one::<Id>("key").unwrap().as_str() {
            "hex" => {
                let hex = args.get_one::<String>("hex").unwrap();
                HexParser::Liberal.decode(hex).expect("hex encoded sturdyref")
            }
            "phrase" => {
                let text = args.get_one::<String>("phrase").unwrap();
                text.as_bytes().to_owned()
            }
            &_ => unreachable!(),
        };
        let m = syndicate::sturdy::SturdyRef::mint(oid, &key);
        println!("{}", TextWriter::encode(&mut NoEmbeddedDomainCodec,
                                          &language().unparse(&m))?);
    }

    Ok(())
}
