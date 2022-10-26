use std::io;
use std::str::FromStr;

use clap::ArgGroup;
use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use clap::arg;
use clap_complete::{generate, Shell};
use preserves::hex::HexParser;
use preserves::value::BytesBinarySource;
use preserves::value::NestedValue;
use preserves::value::NoEmbeddedDomainCodec;
use preserves::value::Reader;
use preserves::value::TextReader;
use preserves::value::ViaCodec;
use preserves::value::TextWriter;
use syndicate::language;
use syndicate::preserves_schema::Codec;
use syndicate::preserves_schema::ParseError;
use syndicate::sturdy::_Any;

#[derive(Clone, Debug)]
struct Preserves<N: NestedValue>(N);

#[derive(Subcommand, Debug)]
enum Action {
    #[command(group(ArgGroup::new("key").required(true)))]
    /// Generate a fresh SturdyRef from an OID value and a key
    Mint {
        #[arg(long, value_name="VALUE")]
        /// Preserves value to use as SturdyRef OID
        oid: Preserves<_Any>,

        #[arg(long, group="key")]
        /// Key phrase
        phrase: Option<String>,

        #[arg(long, group="key")]
        /// Key bytes, encoded as hex
        hex: Option<String>,
    },

    /// Emit shell completion code
    Completions {
        /// Shell dialect to generate
        shell: Shell,
    }
}

#[derive(Parser, Debug)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    action: Action,
}

impl<N: NestedValue> FromStr for Preserves<N> {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Preserves(TextReader::new(&mut BytesBinarySource::new(s.as_bytes()),
                                     ViaCodec::new(NoEmbeddedDomainCodec)).demand_next(false)?))
    }
}

fn main() -> io::Result<()> {
    let args = <Cli as Parser>::parse();

    match args.action {
        Action::Completions { shell } => {
            let mut cmd = <Cli as CommandFactory>::command();
            let name = cmd.get_name().to_string();
            generate(shell, &mut cmd, name, &mut io::stdout());
        }

        Action::Mint { oid, phrase, hex } => {
            let key =
                if let Some(hex) = hex {
                    HexParser::Liberal.decode(&hex).expect("hex encoded sturdyref")
                } else if let Some(phrase) = phrase {
                    phrase.as_bytes().to_owned()
                } else {
                    unreachable!()
                };
            let m = syndicate::sturdy::SturdyRef::mint(oid.0, &key);
            println!("{}", TextWriter::encode(&mut NoEmbeddedDomainCodec,
                                              &language().unparse(&m))?);
        }
    }

    Ok(())
}
