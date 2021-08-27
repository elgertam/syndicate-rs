use std::sync::Arc;

use syndicate::actor::*;
use syndicate::relay;

use crate::protocol::run_io_relay;

pub fn spawn(t: &mut Activation, ds: Arc<Cap>) {
    t.spawn(syndicate::name!("parent"), move |t| run_io_relay(
        t,
        relay::Input::Bytes(Box::pin(tokio::io::stdin())),
        relay::Output::Bytes(Box::pin(tokio::io::stdout())),
        ds))
}
