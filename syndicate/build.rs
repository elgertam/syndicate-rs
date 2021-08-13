use preserves_schema::compiler::*;

mod syndicate_plugins {
    use preserves_schema::compiler::*;
    use preserves_schema::gen::schema::*;
    // use preserves_schema::syntax::block::constructors::*;

    #[derive(Debug)]
    pub(super) struct PatternPlugin;

    impl Plugin for PatternPlugin {
        fn generate(
            &self,
            m: &mut context::ModuleContext,
            _definition_name: &str,
            _definition: &Definition,
        ) {
            if m.mode.is_some() {
                return;
            }

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
    c.module_aliases.insert(vec!["EntityRef".to_owned()], "crate::actor".to_owned());

    let inputs = expand_inputs(&vec!["protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs)?;
    compile(&c)
}
