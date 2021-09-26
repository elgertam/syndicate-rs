use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use tokio::process;

use crate::language::language;
use crate::schemas::external_services::*;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, config_ds, language(), <run-service $spec: DaemonService>, |t| {
            Ok(Supervisor::start(
                t,
                syndicate::name!(parent: None, "daemon", service = ?spec),
                SupervisorConfiguration::default(),
                enclose!((config_ds, root_ds) move |t|
                         enclose!((config_ds, root_ds, spec) run(t, config_ds, root_ds, spec)))))
        }))
    });
}

fn cannot_start() -> ActorResult {
    Err("Cannot start daemon process")?
}

impl DaemonSpec {
    fn elaborate(self) -> FullDaemonSpec {
        match self {
            DaemonSpec::Simple(command_line) => FullDaemonSpec {
                argv: *command_line,
                env: DaemonEnv::Absent,
                dir: DaemonDir::Absent,
                clear_env: ClearEnv::Absent,
            },
            DaemonSpec::Full(spec) => *spec,
        }
    }
}

impl CommandLine {
    fn elaborate(self) -> FullCommandLine {
        match self {
            CommandLine::Shell(s) => FullCommandLine {
                program: "sh".to_owned(),
                args: vec!["-c".to_owned(), s],
            },
            CommandLine::Full(command_line) => *command_line,
        }
    }
}

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    _root_ds: Arc<Cap>,
    service: DaemonService,
) -> ActorResult {
    let spec = language().unparse(&service);

    Ok(during!(t, config_ds, language(), <daemon #(service.id.0) $config>, {
        let spec = spec.clone();
        let config_ds = Arc::clone(&config_ds);
        |t: &mut Activation| match language().parse::<DaemonSpec>(&config) {
            Ok(config) => {
                tracing::info!(?config);
                let config = config.elaborate();
                let facet = t.facet.clone();
                t.linked_task(syndicate::name!("subprocess"), async move {
                    let argv = config.argv.elaborate();
                    let mut cmd = process::Command::new(argv.program);
                    cmd.args(argv.args);
                    match config.dir {
                        DaemonDir::Present { dir } => { cmd.current_dir(dir); () },
                        DaemonDir::Absent => (),
                        DaemonDir::Invalid { dir } => {
                            tracing::error!(?dir, "Invalid working directory");
                            return cannot_start();
                        }
                    }
                    match config.clear_env {
                        ClearEnv::Present { clear_env: true } => { cmd.env_clear(); () },
                        ClearEnv::Present { clear_env: false } => (),
                        ClearEnv::Absent => (),
                        ClearEnv::Invalid { clear_env } => {
                            tracing::error!(?clear_env, "Invalid clearEnv setting");
                            return cannot_start();
                        }
                    }
                    match config.env {
                        DaemonEnv::Present { env } => {
                            for (k, v) in env {
                                if let Some(env_variable) = match k {
                                    EnvVariable::String(k) => Some(k),
                                    EnvVariable::Symbol(k) => Some(k),
                                    EnvVariable::Invalid(env_variable) => {
                                        tracing::error!(?env_variable,
                                                        "Invalid environment variable name");
                                        return cannot_start();
                                    }
                                } {
                                    match v {
                                        EnvValue::Set(value) => { cmd.env(env_variable, value); () }
                                        EnvValue::Remove => { cmd.env_remove(env_variable); () }
                                        EnvValue::Invalid(value) => {
                                            tracing::error!(?env_variable, ?value,
                                                            "Invalid environment variable value");
                                            return cannot_start();
                                        }
                                    }
                                }
                            }
                        }
                        DaemonEnv::Absent => (),
                        DaemonEnv::Invalid { env } => {
                            tracing::error!(?env, "Invalid daemon environment");
                            return cannot_start();
                        }
                    }

                    cmd.stdin(std::process::Stdio::null());
                    cmd.stdout(std::process::Stdio::inherit());
                    cmd.stderr(std::process::Stdio::inherit());
                    cmd.kill_on_drop(true);

                    tracing::info!(?cmd);
                    let mut child = cmd.spawn()?;
                    tracing::info!(pid = ?child.id());

                    facet.activate(
                        Account::new(syndicate::name!("announce-service-running")),
                        |t| {
                            config_ds.assert(t, &(), &syndicate_macros::template!("<service-running =spec>"));
                            Ok(())
                        })?;

                    let status = child.wait().await;
                    tracing::info!(?status);

                    Ok(())
                });
                Ok(())
            }
            Err(_) => {
                tracing::error!(?config, "Invalid DaemonSpec");
                return cannot_start();
            }
        }
    }))
}
