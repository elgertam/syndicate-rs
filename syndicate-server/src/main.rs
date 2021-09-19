use preserves_schema::Codec;

use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::dataspace::*;
use syndicate::relay;
use syndicate::schemas::service;
use syndicate::schemas::transport_address;

use syndicate::value::NestedValue;

mod gatekeeper;
mod language;
mod protocol;
mod services;

mod schemas {
    include!(concat!(env!("OUT_DIR"), "/src/schemas/mod.rs"));
}

use language::language;
use schemas::internal_services;

#[derive(Clone, StructOpt)]
struct ServerConfig {
    #[structopt(short = "p", long = "port")]
    ports: Vec<u16>,

    #[structopt(short = "s", long = "socket")]
    sockets: Vec<PathBuf>,

    #[structopt(long)]
    inferior: bool,

    #[structopt(long)]
    debt_reporter: bool,

    #[structopt(short = "c", long)]
    config: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    syndicate::convenient_logging()?;

    let config = Arc::new(ServerConfig::from_args());

    {
        const BRIGHT_GREEN: &str = "\x1b[92m";
        const RED: &str = "\x1b[31m";
        const GREEN: &str = "\x1b[32m";
        const NORMAL: &str = "\x1b[0m";
        const BRIGHT_YELLOW: &str = "\x1b[93m";

        tracing::info!(r"{}    ______   {}", GREEN, NORMAL);
        tracing::info!(r"{}   /    {}\_{}\{}  ", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{}  /  {},{}__/{}  \ {}                         ____           __", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{} /{}\__/  \{},{}  \{}   _______  ______  ____/ /_/________  / /____", GREEN, BRIGHT_GREEN, RED, GREEN, NORMAL);
        tracing::info!(r"{} \{}/  \__/   {}/{}  / ___/ / / / __ \/ __  / / ___/ __ \/ __/ _ \", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{}  \  {}'{}  \__{}/ {} _\_ \/ /_/ / / / / /_/ / / /__/ /_/ / /_/  __/", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"{}   \____{}/{}_/ {} /____/\__, /_/ /_/\____/_/\___/\__/_/\__/\___/", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        tracing::info!(r"                  /____/");
        tracing::info!(r"");
        tracing::info!(r" {}version {}{}", BRIGHT_YELLOW, env!("CARGO_PKG_VERSION"), NORMAL);
        tracing::info!(r"");
        tracing::info!(r" documentation & reference material: https://syndicate-lang.org/");
        tracing::info!(r" source code & bugs: https://git.syndicate-lang.org/syndicate-lang/syndicate-rs");
        tracing::info!(r"");
    }

    tracing::trace!("startup");

    Actor::new().boot(tracing::Span::current(), move |t| {
        let root_ds = Cap::new(&t.create(Dataspace::new()));

        if config.inferior {
            tracing::info!("inferior server instance");
            let root_ds = Arc::clone(&root_ds);
            t.spawn(syndicate::name!("parent"), move |t| protocol::run_io_relay(
                t,
                relay::Input::Bytes(Box::pin(tokio::io::stdin())),
                relay::Output::Bytes(Box::pin(tokio::io::stdout())),
                root_ds));
        }

        let server_config_ds = Cap::new(&t.create(Dataspace::new()));

        gatekeeper::bind(t, &root_ds, AnyValue::new("syndicate"), [0; 16],
                         Arc::clone(&root_ds));
        gatekeeper::bind(t, &root_ds, AnyValue::new("server-config"), [0; 16],
                         Arc::clone(&server_config_ds));

        let gateway = Cap::guard(Arc::clone(&language().syndicate), t.create(
            syndicate::entity(Arc::clone(&root_ds)).on_asserted(gatekeeper::handle_resolve)));

        services::debt_reporter::on_demand(t, Arc::clone(&server_config_ds));
        services::tcp_relay_listener::on_demand(t, Arc::clone(&server_config_ds), Arc::clone(&gateway));
        services::unix_relay_listener::on_demand(t, Arc::clone(&server_config_ds), Arc::clone(&gateway));
        services::config_watcher::on_demand(t, Arc::clone(&server_config_ds));
        services::daemon::on_demand(t, Arc::clone(&server_config_ds), Arc::clone(&root_ds));

        if config.debt_reporter {
            server_config_ds.assert(t, language(), &service::RequireService {
                service_name: language().unparse(&internal_services::DebtReporter),
            });
        }

        for port in config.ports.clone() {
            server_config_ds.assert(t, language(), &service::RequireService {
                service_name: language().unparse(&internal_services::TcpRelayListener {
                    addr: transport_address::Tcp {
                        host: "0.0.0.0".to_owned(),
                        port: (port as i32).into(),
                    }
                }),
            });
        }

        for path in config.sockets.clone() {
            server_config_ds.assert(t, language(), &service::RequireService {
                service_name: language().unparse(&internal_services::UnixRelayListener {
                    addr: transport_address::Unix {
                        path: path.to_str().expect("representable UnixListener path").to_owned(),
                    }
                }),
            });
        }

        for path in config.config.clone() {
            server_config_ds.assert(t, language(), &service::RequireService {
                service_name: language().unparse(&internal_services::ConfigWatcher {
                    path: path.to_str().expect("representable ConfigWatcher path").to_owned(),
                }),
            });
        }

        Ok(())
    }).await??;

    Ok(())
}
