use preserves_schema::compiler::*;

fn main() -> std::io::Result<()> {
    let buildroot = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new("crate::schemas".to_owned());
    c.add_external_module(ExternalModule::new(vec!["EntityRef".to_owned()], "crate::placeholder"));

    let inputs = expand_inputs(&vec!["../syndicate/protocols/schema-bundle.bin".to_owned()])?;
    c.load_schemas_and_bundles(&inputs, &vec![])?;
    compile(&c, &mut CodeCollector::files(gen_dir))
}
