use std::sync::Arc;

use syndicate::actor::*;
use syndicate::schemas::service::*;
use syndicate::preserves_schema::support::Unparse;

use syndicate_macros::on_message;

pub fn updater<N: Unparse<Arc<Cap>>>(
    ds: Arc<Cap>,
    name: N,
) -> impl FnMut(&mut Activation, State) -> ActorResult {
    let mut handle = None;
    move |t, state| {
        ds.update(t, &mut handle, Some(&lifecycle(&name, state)));
        Ok(())
    }
}

pub fn lifecycle<N: Unparse<Arc<Cap>>>(
    service_name: &N,
    state: State,
) -> ServiceState {
    ServiceState {
        service_name: service_name.unparse(),
        state,
    }
}

pub fn started<N: Unparse<Arc<Cap>>>(service_name: &N) -> ServiceState {
    lifecycle(service_name, State::Started)
}

pub fn ready<N: Unparse<Arc<Cap>>>(service_name: &N) -> ServiceState {
    lifecycle(service_name, State::Ready)
}

pub fn on_service_restart<N: Unparse<Arc<Cap>>,
                          F: 'static + Send + FnMut(&mut Activation) -> ActorResult>(
    t: &mut Activation,
    ds: &Arc<Cap>,
    service_name: &N,
    mut f: F,
) {
    on_message!(t, ds, <restart-service #(&service_name.unparse())>, f);
}

pub fn terminate_on_service_restart<N: Unparse<Arc<Cap>>>(
    t: &mut Activation,
    ds: &Arc<Cap>,
    service_name: &N,
) {
    on_service_restart(t, ds, service_name, |t| {
        tracing::info!("Terminating to restart");
        t.stop_root();
        Ok(())
    });
}
