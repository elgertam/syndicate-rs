use std::sync::Arc;
use preserves::value::ArcValue;
use preserves::value::Domain;

mod schemas {
    //! Auto-generated codecs for [Syndicate protocol
    //! schemas](https://git.syndicate-lang.org/syndicate-lang/syndicate-protocols/src/branch/main/schemas).
    include!(concat!(env!("OUT_DIR"), "/src/schemas/mod.rs"));
}

mod placeholder {
    pub type Cap = super::Cap;
}

#[derive(Debug, Clone, Hash, Ord, Eq, PartialOrd, PartialEq)]
pub enum Cap {}
impl Domain for Cap {}

preserves_schema::define_language!(language(): Language<ArcValue<Arc<Cap>>> {
    syndicate: schemas::Language,
});

mod pattern;
mod pattern_plugin;
pub use pattern_plugin::PatternPlugin;
