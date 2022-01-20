use preserves_schema::compiler::*;

mod syndicate_plugins {
    use preserves_schema::compiler::*;
    use preserves_schema::gen::schema::*;
    // use preserves_schema::syntax::block::constructors::*;

    #[derive(Debug)]
    pub(super) struct PatternPlugin;

    impl Plugin for PatternPlugin {
        fn generate_definition(
            &self,
            _m: &mut context::ModuleContext,
            _definition_name: &str,
            _definition: &Definition,
        ) {
            // TODO: Emit code for building instances of sturdy.Pattern and sturdy.Template
        }
    }
}

fn main() -> std::io::Result<()> {
    let buildroot = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new(gen_dir, "crate::schemas".to_owned());
    c.plugins.push(Box::new(syndicate_plugins::PatternPlugin));
    c.add_external_module(ExternalModule::new(vec!["EntityRef".to_owned()], "crate::actor"));

    let inputs = expand_inputs(&vec!["../../syndicate-protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs)?;
    compile(&c)
}
