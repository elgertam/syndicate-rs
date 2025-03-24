use preserves_schema::compiler::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let buildroot = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new("crate::schemas".to_owned());
    c.plugins.push(Box::new(syndicate_schema_plugin::PatternPlugin::new()));
    c.add_external_module(ExternalModule::new(vec!["EntityRef".to_owned()], "syndicate::actor"));
    c.add_external_module(
        ExternalModule::new(vec!["TransportAddress".to_owned()],
                            "syndicate::schemas::transport_address"));
    c.add_external_module(
        ExternalModule::new(vec!["gatekeeper".to_owned()], "syndicate::schemas::gatekeeper"));
    c.add_external_module(
        ExternalModule::new(vec!["noise".to_owned()], "syndicate::schemas::noise"));

    let inputs = expand_inputs(&vec!["protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs, &vec![])?;
    c.load_xref_bin("syndicate", syndicate::schemas::_bundle())?;
    Ok(compile(&c, &mut CodeCollector::files(gen_dir))?)
}
