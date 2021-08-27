use syndicate::actor::*;

pub fn spawn(t: &mut Activation) {
    t.spawn(syndicate::name!("debt-reporter"), |t| {
        t.linked_task(syndicate::name!("tick"), async {
            let mut timer = tokio::time::interval(core::time::Duration::from_secs(1));
            loop {
                timer.tick().await;
                for (id, (name, debt)) in syndicate::actor::ACCOUNTS.read().unwrap().iter() {
                    let _enter = name.enter();
                    tracing::info!(id, debt = debug(
                        debt.load(std::sync::atomic::Ordering::Relaxed)));
                }
            }
        });
        Ok(())
    });
}
