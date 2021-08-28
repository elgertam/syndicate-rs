use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::dataspace::*;

use syndicate::value::NestedValue;

mod gatekeeper;
mod protocol;
mod services;

#[derive(Clone, StructOpt)]
struct ServerConfig {
    #[structopt(short = "p", long = "port", default_value = "8001")]
    ports: Vec<u16>,

    #[structopt(short = "s", long = "socket")]
    sockets: Vec<PathBuf>,

    #[structopt(long)]
    inferior: bool,

    #[structopt(long)]
    debt_reporter: bool,
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
        let server_config_ds = Cap::new(&t.create(Dataspace::new()));

        gatekeeper::bind(t, &root_ds, AnyValue::new("syndicate"), [0; 16],
                         Arc::clone(&root_ds));
        gatekeeper::bind(t, &root_ds, AnyValue::new("server-config"), [0; 16],
                         Arc::clone(&server_config_ds));

        services::debt_reporter::on_demand(t, Arc::clone(&server_config_ds));
        if config.debt_reporter {
            server_config_ds.assert(t, &syndicate::schemas::service::RequireService {
                service_name: AnyValue::symbol("debt-reporter")
            });
        }

        if config.inferior {
            services::stdio_relay_listener::spawn(t, Arc::clone(&root_ds));
        }

        let gateway = Cap::guard(&t.create(
            syndicate::entity(Arc::clone(&root_ds)).on_asserted(gatekeeper::handle_resolve)));

        for port in config.ports.clone() {
            services::tcp_relay_listener::spawn(t, Arc::clone(&gateway), port);
        }

        for path in config.sockets.clone() {
            services::unix_relay_listener::spawn(t, Arc::clone(&gateway), path);
        }

        Ok(())
    }).await??;

    Ok(())
}
