use std::sync::Arc;
use syndicate::actor::Cap;

preserves_schema::define_language!(language(): Language<Arc<Cap>> {
    syndicate: syndicate::schemas::Language,
    server: crate::schemas::Language,
});
