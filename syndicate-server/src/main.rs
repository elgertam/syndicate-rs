use preserves_schema::Codec;

use std::convert::TryInto;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;

use syndicate::actor::*;
use syndicate::dataspace::*;
use syndicate::enclose;
use syndicate::relay;
use syndicate::schemas::service;
use syndicate::schemas::transport_address;
use syndicate::trace;

use syndicate::value::Map;
use syndicate::value::NestedValue;

mod counter;
mod dependencies;
mod gatekeeper;
mod http;
mod language;
mod lifecycle;
mod protocol;
mod script;
mod services;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod schemas {
    include!(concat!(env!("OUT_DIR"), "/src/schemas/mod.rs"));
}

use language::Language;
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

    #[structopt(long)]
    no_banner: bool,

    #[structopt(short = "t", long)]
    trace_file: Option<PathBuf>,

    /// Enable `$control` entity.
    #[structopt(long)]
    control: bool,
}

#[tokio::main]
async fn main() -> ActorResult {
    let config = Arc::new(ServerConfig::from_args());

    syndicate::convenient_logging()?;

    if !config.no_banner && !config.inferior {
        const BRIGHT_GREEN: &str = "\x1b[92m";
        const RED: &str = "\x1b[31m";
        const GREEN: &str = "\x1b[32m";
        const NORMAL: &str = "\x1b[0m";
        const BRIGHT_YELLOW: &str = "\x1b[93m";

        eprintln!(r"{}    ______   {}", GREEN, NORMAL);
        eprintln!(r"{}   /    {}\_{}\{}  ", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        eprintln!(r"{}  /  {},{}__/{}  \ {}                         ____           __", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        eprintln!(r"{} /{}\__/  \{},{}  \{}   _______  ______  ____/ /_/________  / /____", GREEN, BRIGHT_GREEN, RED, GREEN, NORMAL);
        eprintln!(r"{} \{}/  \__/   {}/{}  / ___/ / / / __ \/ __  / / ___/ __ \/ __/ _ \", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        eprintln!(r"{}  \  {}'{}  \__{}/ {} _\_ \/ /_/ / / / / /_/ / / /__/ /_/ / /_/  __/", GREEN, RED, BRIGHT_GREEN, GREEN, NORMAL);
        eprintln!(r"{}   \____{}/{}_/ {} /____/\__, /_/ /_/\____/_/\___/\__/_/\__/\___/", GREEN, BRIGHT_GREEN, GREEN, NORMAL);
        eprintln!(r"                  /____/");
        eprintln!(r"");
        eprintln!(r" {}version {} [syndicate {}]{}", BRIGHT_YELLOW, env!("CARGO_PKG_VERSION"), syndicate::syndicate_package_version(), NORMAL);
        eprintln!(r"");
        eprintln!(r" documentation & reference material: https://syndicate-lang.org/");
        eprintln!(r" source code & bugs: https://git.syndicate-lang.org/syndicate-lang/syndicate-rs");
        eprintln!(r"");
    }

    tracing::trace!("startup");

    let trace_collector = config.trace_file.clone().map(
        |p| Ok::<trace::TraceCollector, io::Error>(trace::TraceCollector::ascii(
            io::BufWriter::new(std::fs::File::create(p)?))))
        .transpose()?;

    Actor::top(trace_collector, move |t| {
        let server_config_ds = Cap::new(&t.create(Dataspace::new(Some(AnyValue::symbol("config")))));
        let log_ds = Cap::new(&t.create(Dataspace::new(Some(AnyValue::symbol("log")))));

        if config.inferior {
            tracing::info!("inferior server instance");
            t.spawn(Some(AnyValue::symbol("parent")), enclose!((server_config_ds) move |t| {
                protocol::run_io_relay(t,
                                       relay::Input::Bytes(Box::pin(tokio::io::stdin())),
                                       relay::Output::Bytes(Box::pin(tokio::io::stdout())),
                                       server_config_ds)
            }));
        }

        let gatekeeper = Cap::guard(Language::arc(), t.create(
            syndicate::entity(Arc::clone(&server_config_ds))
                .on_asserted_facet(gatekeeper::facet_handle_resolve)));
        gatekeeper::handle_binds(t, &server_config_ds)?;

        let mut env = Map::new();
        env.insert("config".to_owned(), AnyValue::domain(Arc::clone(&server_config_ds)));
        env.insert("log".to_owned(), AnyValue::domain(Arc::clone(&log_ds)));
        env.insert("gatekeeper".to_owned(), AnyValue::domain(Arc::clone(&gatekeeper)));

        if config.control {
            env.insert("control".to_owned(), AnyValue::domain(Cap::guard(Language::arc(), t.create(
                syndicate::entity(())
                    .on_message(|_, _t, m: crate::schemas::control::ExitServer| {
                        tracing::info!("$control received exit request with code {}", m.code);
                        std::process::exit((&m.code).try_into().unwrap_or_else(|_| {
                            tracing::warn!(
                                "exit code {} out-of-range of 32-bit signed integer, using 1 instead",
                                m.code);
                            1
                        }))
                    })))));
        }

        dependencies::boot(t, Arc::clone(&server_config_ds));
        services::config_watcher::on_demand(t, Arc::clone(&server_config_ds));
        services::daemon::on_demand(t, Arc::clone(&server_config_ds), Arc::clone(&log_ds));
        services::debt_reporter::on_demand(t, Arc::clone(&server_config_ds));
        services::gatekeeper::on_demand(t, Arc::clone(&server_config_ds));
        services::http_router::on_demand(t, Arc::clone(&server_config_ds));
        services::tcp_relay_listener::on_demand(t, Arc::clone(&server_config_ds));
        services::unix_relay_listener::on_demand(t, Arc::clone(&server_config_ds));

        if config.debt_reporter {
            server_config_ds.assert(t, language(), &service::RunService {
                service_name: language().unparse(&internal_services::DebtReporter {
                    interval_seconds: (1.0).into(),
                }),
            });
        }

        for port in config.ports.clone() {
            server_config_ds.assert(t, language(), &service::RunService {
                service_name: language().unparse(&internal_services::TcpWithoutHttp {
                    addr: transport_address::Tcp {
                        host: "0.0.0.0".to_owned(),
                        port: (port as i32).into(),
                    },
                    gatekeeper: gatekeeper.clone(),
                }),
            });
        }

        for path in config.sockets.clone() {
            server_config_ds.assert(t, language(), &service::RunService {
                service_name: language().unparse(&internal_services::UnixRelayListener {
                    addr: transport_address::Unix {
                        path: path.to_str().expect("representable UnixListener path").to_owned(),
                    },
                    gatekeeper: gatekeeper.clone(),
                }),
            });
        }

        for path in config.config.clone() {
            server_config_ds.assert(t, language(), &service::RunService {
                service_name: language().unparse(&internal_services::ConfigWatcher {
                    path: path.to_str().expect("representable ConfigWatcher path").to_owned(),
                    env: internal_services::ConfigEnv(env.clone()),
                }),
            });
        }

        t.spawn(Some(AnyValue::symbol("logger")), enclose!((log_ds) move |t| {
            let n_unknown: AnyValue = AnyValue::symbol("-");
            let n_pid: AnyValue = AnyValue::symbol("pid");
            let n_line: AnyValue = AnyValue::symbol("line");
            let n_service: AnyValue = AnyValue::symbol("service");
            let n_stream: AnyValue = AnyValue::symbol("stream");
            let e = syndicate::during::entity(())
                .on_message(move |(), _t, captures: AnyValue| {
                    if let Some(captures) = captures.value_owned().into_sequence() {
                        let mut captures = captures.into_iter();
                        let timestamp = captures.next()
                            .and_then(|t| t.value_owned().into_string())
                            .unwrap_or_else(|| "-".to_owned());
                        if let Some(mut d) = captures.next()
                            .and_then(|d| d.value_owned().into_dictionary())
                        {
                            let pid = d.remove(&n_pid).unwrap_or_else(|| n_unknown.clone());
                            let service = d.remove(&n_service).unwrap_or_else(|| n_unknown.clone());
                            let line = d.remove(&n_line).unwrap_or_else(|| n_unknown.clone());
                            let stream = d.remove(&n_stream).unwrap_or_else(|| n_unknown.clone());
                            let message = format!("{} {:?}[{:?}] {:?}: {:?}",
                                                  timestamp,
                                                  service,
                                                  pid,
                                                  stream,
                                                  line);
                            if d.is_empty() {
                                tracing::info!(target: "", "{}", message);
                            } else {
                                tracing::info!(target: "",  "{} {:?}", message, d);
                            }
                        }
                    }
                    Ok(())
                })
                .create_cap(t);
            log_ds.assert(t, language(), &syndicate::schemas::dataspace::Observe {
                pattern: syndicate_macros::pattern!(<log $ $>),
                observer: e,
            });
            Ok(())
        }));

        Ok(())
    }).await??;

    wait_for_all_actors_to_stop(std::time::Duration::from_secs(2)).await;

    Ok(())
}
