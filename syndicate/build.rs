use preserves_schema::compiler::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let buildroot = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new("crate::schemas".to_owned());
    c.plugins.push(Box::new(syndicate_schema_plugin::PatternPlugin {
        syndicate_crate: "crate".to_string(),
    }));
    c.add_external_module(ExternalModule::new(vec!["EntityRef".to_owned()], "crate::actor"));

    let inputs = expand_inputs(&vec!["protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs, &vec![])?;
    Ok(compile(&c, &mut CodeCollector::files(gen_dir))?)
}
