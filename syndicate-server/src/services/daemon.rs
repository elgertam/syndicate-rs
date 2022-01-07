use preserves_schema::Codec;

use std::sync::Arc;

use syndicate::actor::*;
use syndicate::enclose;
use syndicate::schemas::service;
use syndicate::supervise::{Supervisor, SupervisorConfiguration};
use syndicate::value::NestedValue;

use tokio::io::AsyncRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process;

use crate::counter;
use crate::language::language;
use crate::lifecycle;
use crate::schemas::external_services::*;

use syndicate_macros::during;

pub fn on_demand(t: &mut Activation, config_ds: Arc<Cap>, root_ds: Arc<Cap>) {
    t.spawn(syndicate::name!("daemon"), move |t| {
        Ok(during!(t, config_ds, language(), <run-service $spec: DaemonService>, |t| {
            Supervisor::start(
                t,
                syndicate::name!(parent: None, "daemon", id = ?spec.id),
                SupervisorConfiguration::default(),
                enclose!((config_ds, spec) lifecycle::updater(config_ds, spec)),
                enclose!((config_ds, root_ds) move |t|
                         enclose!((config_ds, root_ds, spec) run(t, config_ds, root_ds, spec))))
        }))
    });
}

impl Process {
    fn elaborate(self) -> FullProcess {
        match self {
            Process::Simple(command_line) => FullProcess {
                argv: *command_line,
                env: ProcessEnv::Absent,
                dir: ProcessDir::Absent,
                clear_env: ClearEnv::Absent,
            },
            Process::Full(spec) => *spec,
        }
    }
}

impl FullProcess {
    fn build_command(&self) -> Option<process::Command> {
        let argv = self.argv.clone().elaborate();
        let mut cmd = process::Command::new(argv.program);
        cmd.args(argv.args);
        match &self.dir {
            ProcessDir::Present { dir } => { cmd.current_dir(dir); () },
            ProcessDir::Absent => (),
            ProcessDir::Invalid { dir } => {
                tracing::error!(?dir, "Invalid working directory");
                return None;
            }
        }
        match &self.clear_env {
            ClearEnv::Present { clear_env: true } => { cmd.env_clear(); () },
            ClearEnv::Present { clear_env: false } => (),
            ClearEnv::Absent => (),
            ClearEnv::Invalid { clear_env } => {
                tracing::error!(?clear_env, "Invalid clearEnv setting");
                return None;
            }
        }
        match &self.env {
            ProcessEnv::Present { env } => {
                for (k, v) in env {
                    if let Some(env_variable) = match k {
                        EnvVariable::String(k) => Some(k),
                        EnvVariable::Symbol(k) => Some(k),
                        EnvVariable::Invalid(env_variable) => {
                            tracing::error!(?env_variable,
                                            "Invalid environment variable name");
                            return None;
                        }
                    } {
                        match v {
                            EnvValue::Set(value) => { cmd.env(env_variable, value); () }
                            EnvValue::Remove => { cmd.env_remove(env_variable); () }
                            EnvValue::Invalid(value) => {
                                tracing::error!(?env_variable, ?value,
                                                "Invalid environment variable value");
                                return None;
                            }
                        }
                    }
                }
            }
            ProcessEnv::Absent => (),
            ProcessEnv::Invalid { env } => {
                tracing::error!(?env, "Invalid daemon environment");
                return None;
            }
        }
        cmd.kill_on_drop(true);
        Some(cmd)
    }
}

impl DaemonProcessSpec {
    fn elaborate(self) -> FullDaemonProcess {
        match self {
            DaemonProcessSpec::Simple(command_line) => FullDaemonProcess {
                process: Process::Simple(command_line).elaborate(),
                ready_on_start: ReadyOnStart::Absent,
                restart: RestartField::Absent,
                protocol: ProtocolField::Absent,
            },
            DaemonProcessSpec::Full(spec) => *spec,
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

struct DaemonInstance {
    config_ds: Arc<Cap>,
    log_ds: Arc<Cap>,
    service: AnyValue,
    name: tracing::Span,
    cmd: process::Command,
    announce_presumed_readiness: bool,
    unready_configs: Arc<Field<isize>>,
    completed_processes: Arc<Field<isize>>,
    restart_policy: RestartPolicy,
    protocol: Protocol,
}

impl DaemonInstance {
    fn handle_exit(self, t: &mut Activation, error_message: Option<String>) -> ActorResult {
        let delay =
            std::time::Duration::from_millis(if let None = error_message { 200 } else { 1000 });
        t.stop_facet_and_continue(t.facet.facet_id, Some(move |t: &mut Activation| {
            #[derive(Debug)]
            enum NextStep {
                SleepAndRestart,
                SignalSuccessfulCompletion,
            }
            use NextStep::*;

            let next_step = match self.restart_policy {
                RestartPolicy::Always => SleepAndRestart,
                RestartPolicy::OnError =>
                    match &error_message {
                        None => SignalSuccessfulCompletion,
                        Some(_) => SleepAndRestart,
                    },
                RestartPolicy::All =>
                    match &error_message {
                        None => SignalSuccessfulCompletion,
                        Some(s) => {
                            tracing::error!(cmd = ?self.cmd, next_step = %"RestartDaemon", message = %s);
                            Err(s.as_str())?
                        }
                    },
            };

            match error_message {
                Some(m) => tracing::error!(cmd = ?self.cmd, ?next_step, message = %m),
                None => tracing::info!(cmd = ?self.cmd, ?next_step),
            }

            match next_step {
                SleepAndRestart => t.after(delay, |t| self.start(t)),
                SignalSuccessfulCompletion => {
                    t.facet(|t| {
                        let _ = t.prevent_inert_check();
                        counter::adjust(t, &self.completed_processes, 1);
                        counter::adjust(t, &self.unready_configs, -1);
                        Ok(())
                    })?;
                    ()
                }
            }
            Ok(())
        }))
    }

    fn log<R: 'static + Send + AsyncRead + Unpin>(
        &self,
        t: &mut Activation,
        facet: FacetRef,
        pid: Option<u32>,
        r: R,
        kind: &str
    ) {
        let log_ds = self.log_ds.clone();
        let service = self.service.clone();
        let kind = AnyValue::symbol(kind);
        let pid = match pid {
            Some(n) => AnyValue::new(n),
            None => AnyValue::symbol("unknown"),
        };
        t.spawn(syndicate::name!(parent: self.name.clone(), "log"), move |t| {
            t.linked_task(tracing::Span::current(), async move {
                let mut r = BufReader::new(r);
                loop {
                    let mut buf = Vec::new();
                    if r.read_until(b'\n', &mut buf).await? == 0 {
                        return Ok(LinkedTaskTermination::Normal);
                    }
                    let buf = match std::str::from_utf8(&buf) {
                        Ok(s) => AnyValue::new(s),
                        Err(_) => AnyValue::bytestring(buf),
                    };
                    let now = AnyValue::new(chrono::Utc::now().to_rfc3339());
                    if facet.activate(
                        Account::new(tracing::Span::current()),
                        enclose!((pid, service, kind) |t| {
                            log_ds.message(t, &(), &syndicate_macros::template!(
                                "<log =now {
                                             pid: =pid,
                                             service: =service,
                                             stream: =kind,
                                             line: =buf,
                                           }>"));
                            Ok(())
                        })).is_err()
                    {
                        break;
                    }
                }
                Ok(LinkedTaskTermination::Normal)
            });
            Ok(())
        });
    }

    fn start(mut self, t: &mut Activation) -> ActorResult {
        t.facet(|t| {
            tracing::trace!(cmd = ?self.cmd, "starting");
            let mut child = match self.cmd.spawn() {
                Ok(child) => child,
                Err(e) => {
                    tracing::debug!(spawn_err = ?e);
                    return self.handle_exit(t, Some(format!("{}", e)));
                }
            };
            let pid = child.id();
            tracing::debug!(?pid, cmd = ?self.cmd, "started");

            let facet = t.facet.clone();

            if let Some(r) = child.stderr.take() {
                self.log(t, facet.clone(), pid, r, "stderr");
            }

            match self.protocol {
                Protocol::TextSyndicate => self.relay_facet(t, &mut child, true)?,
                Protocol::BinarySyndicate => self.relay_facet(t, &mut child, false)?,
                Protocol::None => {
                    if let Some(r) = child.stdout.take() {
                        self.log(t, facet.clone(), pid, r, "stdout");
                    }
                }
            }

            if self.announce_presumed_readiness {
                counter::adjust(t, &self.unready_configs, -1);
            }

            t.linked_task(
                syndicate::name!(parent: self.name.clone(), "wait"),
                enclose!((facet) async move {
                    tracing::trace!("waiting for process exit");
                    let status = child.wait().await?;
                    tracing::debug!(?status);
                    facet.activate(Account::new(syndicate::name!("instance-terminated")), |t| {
                        let m = if status.success() { None } else { Some(format!("{}", status)) };
                        self.handle_exit(t, m)
                    })?;
                    Ok(LinkedTaskTermination::Normal)
                }));
            Ok(())
        })?;
        Ok(())
    }

    fn relay_facet(&self, t: &mut Activation, child: &mut process::Child, output_text: bool) -> ActorResult {
        use syndicate::relay;
        use syndicate::schemas::sturdy;

        let to_child = child.stdin.take().expect("pipe to child");
        let from_child = child.stdout.take().expect("pipe from child");

        let i = relay::Input::Bytes(Box::pin(from_child));
        let o = relay::Output::Bytes(Box::pin(to_child));

        t.facet(|t| {
            let cap = relay::TunnelRelay::run(t, i, o, None, Some(sturdy::Oid(0.into())), output_text)
                .ok_or("initial capability reference unavailable")?;
            tracing::info!(?cap);
            self.config_ds.assert(t, language(), &service::ServiceObject {
                service_name: self.service.clone(),
                object: AnyValue::domain(cap),
            });
            Ok(())
        })?;
        Ok(())
    }
}

fn run(
    t: &mut Activation,
    config_ds: Arc<Cap>,
    root_ds: Arc<Cap>,
    service: DaemonService,
) -> ActorResult {
    lifecycle::terminate_on_service_restart(t, &config_ds, &service);

    let spec = language().unparse(&service);

    let total_configs = t.named_field("total_configs", 0isize);
    let unready_configs = t.named_field("unready_configs", 1isize);
    let completed_processes = t.named_field("completed_processes", 0isize);

    t.dataflow({
        let mut handle = None;
        let ready = lifecycle::ready(&spec);
        enclose!((config_ds, unready_configs) move |t| {
            let busy_count = *t.get(&unready_configs);
            tracing::debug!(?busy_count);
            config_ds.update(t, &mut handle, language(), if busy_count == 0 { Some(&ready) } else { None });
            Ok(())
        })
    })?;

    t.dataflow(enclose!((completed_processes, total_configs) move |t| {
        let total = *t.get(&total_configs);
        let completed = *t.get(&completed_processes);
        tracing::debug!(total_configs = ?total, completed_processes = ?completed);
        if total > 0 && total == completed {
            t.stop();
        }
        Ok(())
    }))?;

    enclose!((config_ds, unready_configs, completed_processes)
             during!(t, config_ds.clone(), language(), <daemon #(&service.id) $config>, {
                 enclose!((spec, config_ds, root_ds, unready_configs, completed_processes)
                          |t: &mut Activation| {
                              tracing::debug!(?config, "new config");
                              counter::adjust(t, &unready_configs, 1);
                              counter::adjust(t, &total_configs, 1);

                              match language().parse::<DaemonProcessSpec>(&config) {
                                  Ok(config) => {
                                      tracing::info!(?config);
                                      let config = config.elaborate();
                                      let facet = t.facet.clone();
                                      t.linked_task(syndicate::name!("subprocess"), async move {
                                          let mut cmd = config.process.build_command().ok_or("Cannot start daemon process")?;

                                          let announce_presumed_readiness = match config.ready_on_start {
                                              ReadyOnStart::Present { ready_on_start } => ready_on_start,
                                              ReadyOnStart::Absent => true,
                                              ReadyOnStart::Invalid { ready_on_start } => {
                                                  tracing::error!(?ready_on_start, "Invalid readyOnStart value");
                                                  Err("Invalid readyOnStart value")?
                                              }
                                          };
                                          let restart_policy = match config.restart {
                                              RestartField::Present { restart } => *restart,
                                              RestartField::Absent => RestartPolicy::All,
                                              RestartField::Invalid { restart } => {
                                                  tracing::error!(?restart, "Invalid restart value");
                                                  Err("Invalid restart value")?
                                              }
                                          };
                                          let protocol = match config.protocol {
                                              ProtocolField::Present { protocol } => *protocol,
                                              ProtocolField::Absent => Protocol::None,
                                              ProtocolField::Invalid { protocol } => {
                                                  tracing::error!(?protocol, "Invalid protocol value");
                                                  Err("Invalid protocol value")?
                                              }
                                          };

                                          cmd.stdin(match &protocol {
                                              Protocol::None =>
                                                  std::process::Stdio::null(),
                                              Protocol::TextSyndicate | Protocol::BinarySyndicate =>
                                                  std::process::Stdio::piped(),
                                          });
                                          cmd.stdout(std::process::Stdio::piped());
                                          cmd.stderr(std::process::Stdio::piped());

                                          let daemon_instance = DaemonInstance {
                                              config_ds,
                                              log_ds: root_ds,
                                              service: spec,
                                              name: tracing::Span::current(),
                                              cmd,
                                              announce_presumed_readiness,
                                              unready_configs,
                                              completed_processes,
                                              restart_policy,
                                              protocol,
                                          };

                                          facet.activate(Account::new(syndicate::name!("instance-startup")), |t| {
                                              daemon_instance.start(t)
                                          })?;
                                          Ok(LinkedTaskTermination::KeepFacet)
                                      });
                                      Ok(())
                                  }
                                  Err(_) => {
                                      tracing::error!(?config, "Invalid Process specification");
                                      return Ok(());
                                  }
                              }
                          })
             }));

    tracing::debug!("syncing to ds");
    counter::sync_and_adjust(t, &config_ds.underlying, &unready_configs, -1);
    Ok(())
}
