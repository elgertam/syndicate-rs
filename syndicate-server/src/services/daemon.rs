use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::error::Error;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};

use tokio::process;

use crate::counter;
use crate::language::language;
use crate::lifecycle;
use crate::schemas::external_services::*;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    t.spawn(syndicate::name!("on_demand", module = module_path!()), move |t| {
        Ok(during!(t, config_ds, language(), <run-service $spec: DaemonService>, |t| {
            Supervisor::start(
                t,
                syndicate::name!(parent: None, "daemon", service = ?spec),
                SupervisorConfiguration::default(),
                enclose!((config_ds, spec) lifecycle::updater(config_ds, spec)),
                enclose!((config_ds, root_ds) move |t|
                         enclose!((config_ds, root_ds, spec) run(t, config_ds, root_ds, spec))))
        }))
    });
}

fn cannot_start<R>() -> Result<R, Error> {
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
                ready_on_start: ReadyOnStart::Absent,
                restart: RestartField::Absent,
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

struct DaemonProcessInstance {
    name: tracing::Span,
    facet: FacetRef,
    cmd: process::Command,
    announce_presumed_readiness: bool,
    unready_configs: Arc<Field<isize>>,
    restart_policy: RestartPolicy,
}

impl DaemonProcessInstance {
    async fn handle_exit(self, error_message: Option<String>) -> Result<LinkedTaskTermination, Error> {
        let delay_ms = if let None = error_message { 200 } else { 1000 };
        let sleep_after_exit = || tokio::time::sleep(std::time::Duration::from_millis(delay_ms));
        Ok(match self.restart_policy {
            RestartPolicy::Always => {
                sleep_after_exit().await;
                self.start()?;
                LinkedTaskTermination::Normal
            }
            RestartPolicy::OnError => {
                if let None = error_message {
                    LinkedTaskTermination::KeepFacet
                } else {
                    sleep_after_exit().await;
                    self.start()?;
                    LinkedTaskTermination::Normal
                }
            }
            RestartPolicy::All => {
                match error_message {
                    None => LinkedTaskTermination::KeepFacet,
                    Some(s) => Err(s.as_str())?,
                }
            }
        })
    }

    fn start(mut self) -> ActorResult {
        tracing::trace!("DaemonProcessInstance start (outer)");
        self.facet.clone().activate(
            Account::new(syndicate::name!(parent: self.name.clone(), "instance")), |t| {
                tracing::trace!("DaemonProcessInstance start (inner)");
                t.facet(|t| {
                    tracing::trace!(cmd = ?self.cmd, "starting");
                    let mut child = match self.cmd.spawn() {
                        Ok(child) => child,
                        Err(e) => {
                            tracing::info!(spawn_err = ?e);
                            t.linked_task(syndicate::name!(parent: self.name.clone(), "fail"),
                                          self.handle_exit(Some(format!("{}", e))));
                            return Ok(());
                        }
                    };
                    tracing::info!(pid = ?child.id(), cmd = ?self.cmd, "started");

                    if self.announce_presumed_readiness {
                        counter::adjust(t, &self.unready_configs, -1);
                    }

                    t.linked_task(syndicate::name!(parent: self.name.clone(), "wait"), async move {
                        tracing::trace!("waiting for process exit");
                        let status = child.wait().await?;
                        tracing::info!(?status);
                        self.handle_exit(
                            if status.success() { None } else { Some(format!("{}", status)) }).await
                    });
                    Ok(())
                })?;
                Ok(())
            })
    }
}

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    _root_ds: Arc<Cap>,
    service: DaemonService,
) -> ActorResult {
    let spec = language().unparse(&service);

    let unready_configs = t.field(1isize);
    t.dataflow({
        let mut handle = None;
        let ready = lifecycle::ready(&spec);
        enclose!((config_ds, unready_configs) move |t| {
            let busy_count = *t.get(&unready_configs);
            tracing::trace!(?busy_count);
            config_ds.update(t, &mut handle, language(), if busy_count == 0 { Some(&ready) } else { None });
            Ok(())
        })
    })?;

    Ok(during!(t, config_ds, language(), <daemon #(service.id.0) $config>, {
        let unready_configs = unready_configs.clone();
        |t: &mut Activation| {
            counter::adjust(t, &unready_configs, 1);

            match language().parse::<DaemonSpec>(&config) {
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
                        let announce_presumed_readiness = match config.ready_on_start {
                            ReadyOnStart::Present { ready_on_start } => ready_on_start,
                            ReadyOnStart::Absent => true,
                            ReadyOnStart::Invalid { ready_on_start } => {
                                tracing::error!(?ready_on_start, "Invalid readyOnStart value");
                                return cannot_start();
                            }
                        };
                        let restart_policy = match config.restart {
                            RestartField::Present { restart } => *restart,
                            RestartField::Absent => RestartPolicy::All,
                            RestartField::Invalid { restart } => {
                                tracing::error!(?restart, "Invalid restart value");
                                return cannot_start();
                            }
                        };

                        cmd.stdin(std::process::Stdio::null());
                        cmd.stdout(std::process::Stdio::inherit());
                        cmd.stderr(std::process::Stdio::inherit());
                        cmd.kill_on_drop(true);

                        (DaemonProcessInstance {
                            name: tracing::Span::current(),
                            facet,
                            cmd,
                            announce_presumed_readiness,
                            unready_configs,
                            restart_policy,
                        }).start()?;

                        Ok(LinkedTaskTermination::KeepFacet)
                    });
                    Ok(())
                }
                Err(_) => {
                    tracing::error!(?config, "Invalid DaemonSpec");
                    return Ok(());
                }
            }
        }
    }))
}
