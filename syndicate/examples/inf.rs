use syndicate::actor::Actor;
use syndicate::actor::ActorResult;
use syndicate::actor::AnyValue;
use syndicate::actor::Cap;
use syndicate::dataspace::Dataspace;
use syndicate::relay;

#[tokio::main]
async fn main() -> ActorResult {
    syndicate::convenient_logging()?;

    Actor::top(None, move |t| {
        let local_ds = Cap::new(&t.create(Dataspace::new(Some(AnyValue::symbol("pty")))));

        relay::TunnelRelay::run(t,
                                relay::Input::Bytes(Box::pin(tokio::io::stdin())),
                                relay::Output::Bytes(Box::pin(tokio::io::stdout())),
                                Some(local_ds.clone()),
                                None,
                                false);

        Ok(())
    }).await??;

    Ok(())
}
