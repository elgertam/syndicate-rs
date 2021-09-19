use syndicate::actor;

preserves_schema::define_language!(language(): Language<actor::AnyValue> {
    syndicate: syndicate::schemas::Language,
    server: crate::schemas::Language,
});
