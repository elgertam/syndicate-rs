use std::sync::Arc;

use syndicate::actor::*;
use syndicate::schemas::service::*;
use syndicate::preserves_schema::support::Unparse;

use crate::language::Language;
use crate::language::language;

use syndicate_macros::on_message;

pub fn updater<'a, N: Clone + Unparse<&'a Language<AnyValue>, AnyValue>>(
    ds: Arc<Cap>,
    name: N,
) -> impl FnMut(&mut Activation, State) -> ActorResult {
    let mut handle = None;
    move |t, state| {
        ds.update(t, &mut handle, language(), Some(&lifecycle(&name, state)));
        Ok(())
    }
}

pub fn lifecycle<'a, N: Unparse<&'a Language<AnyValue>, AnyValue>>(
    service_name: &N,
    state: State,
) -> ServiceState {
    ServiceState {
        service_name: service_name.unparse(language()),
        state,
    }
}

pub fn started<'a, N: Unparse<&'a Language<AnyValue>, AnyValue>>(service_name: &N) -> ServiceState {
    lifecycle(service_name, State::Started)
}

pub fn ready<'a, N: Unparse<&'a Language<AnyValue>, AnyValue>>(service_name: &N) -> ServiceState {
    lifecycle(service_name, State::Ready)
}

pub fn terminate_on_service_restart<'a, N: Unparse<&'a Language<AnyValue>, AnyValue>>(
    t: &mut Activation,
    ds: &Arc<Cap>,
    service_name: &N,
) {
    on_message!(t, ds, language(), <restart-service #(&service_name.unparse(language()))>, |t: &mut Activation| {
        tracing::info!("Terminating to restart");
        t.state.shutdown();
        Ok(())
    });
}
