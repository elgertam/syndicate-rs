use preserves_schema::compiler::*;

fn main() -> std::io::Result<()> {
    let buildroot = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new(gen_dir, "crate::schemas".to_owned());
    // c.plugins.push(Box::new(syndicate_plugins::PatternPlugin));
    c.add_external_module(ExternalModule::new(vec!["EntityRef".to_owned()], "syndicate::actor"));
    c.add_external_module(
        ExternalModule::new(vec!["TransportAddress".to_owned()],
                            "syndicate::schemas::transport_address")
            .set_fallback_language_types(
                |v| vec![format!("syndicate::schemas::Language<{}>", v)].into_iter().collect()));

    let inputs = expand_inputs(&vec!["protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs)?;
    compile(&c)
}
