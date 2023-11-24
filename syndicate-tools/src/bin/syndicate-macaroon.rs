use std::io;
use std::str::FromStr;

use clap::ArgGroup;
use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use clap::arg;
use clap_complete::{generate, Shell};
use noise_protocol::DH;
use noise_protocol::Hash;
use noise_rust_crypto::Blake2s;
use noise_rust_crypto::X25519;
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
use syndicate::schemas::noise;
use syndicate::sturdy::Caveat;
use syndicate::sturdy::SturdyRef;
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

        #[arg(long)]
        /// Caveats to add
        caveat: Vec<Preserves<_Any>>,
    },

    #[command(group(ArgGroup::new("key").required(true)))]
    /// Generate a fresh NoiseServiceSpec from a service selector and a key
    Noise {
        #[arg(long, value_name="VALUE")]
        /// Preserves value to use as the service selector
        service: Preserves<_Any>,

        #[arg(long, value_name="PROTOCOL")]
        /// Noise handshake protocol name
        protocol: Option<String>,

        #[arg(long, group="key")]
        /// Key phrase
        phrase: Option<String>,

        #[arg(long, group="key")]
        /// Key bytes, encoded as hex
        hex: Option<String>,

        #[arg(long, group="key")]
        /// Generate a random key
        random: bool,
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

        Action::Noise { service, protocol, phrase, hex, random } => {
            let key =
                if random {
                    X25519::genkey()
                } else if let Some(hex) = hex {
                    let mut hash = Blake2s::default();
                    hash.input(hex.as_bytes());
                    hash.result()
                } else if let Some(phrase) = phrase {
                    let mut hash = Blake2s::default();
                    hash.input(phrase.as_bytes());
                    hash.result()
                } else {
                    unreachable!()
                };
            let n = noise::NoiseServiceSpec {
                base: noise::NoiseSpec {
                    key: X25519::pubkey(&key).to_vec(),
                    service: noise::ServiceSelector(service.0),
                    pre_shared_keys: noise::NoisePreSharedKeys::Absent,
                    protocol: if let Some(p) = protocol {
                        noise::NoiseProtocol::Present { protocol: p }
                    } else {
                        noise::NoiseProtocol::Absent
                    },
                },
                secret_key: noise::SecretKeyField::Present {
                    secret_key: key.to_vec(),
                },
            };
            println!("{}", TextWriter::encode(&mut NoEmbeddedDomainCodec,
                                              &language().unparse(&n))?);
        }

        Action::Mint { oid, phrase, hex, caveat: caveats } => {
            let key =
                if let Some(hex) = hex {
                    HexParser::Liberal.decode(&hex).expect("hex encoded sturdyref")
                } else if let Some(phrase) = phrase {
                    phrase.as_bytes().to_owned()
                } else {
                    unreachable!()
                };
            let attenuation = caveats.into_iter().map(|c| {
                let r = language().parse(&c.0);
                if let Ok(Caveat::Unknown(_)) = &r {
                    eprintln!("Warning: Unknown caveat format: {:?}", &c.0);
                }
                r
            }).collect::<Result<Vec<Caveat>, _>>()?;
            let m = SturdyRef::mint(oid.0, &key).attenuate(&attenuation)?;
            println!("{}", TextWriter::encode(&mut NoEmbeddedDomainCodec,
                                              &language().unparse(&m))?);
        }
    }

    Ok(())
}
