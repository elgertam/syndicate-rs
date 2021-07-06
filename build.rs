use preserves_schema::compiler::*;

use std::io::Error;

fn main() -> Result<(), Error> {
    let buildroot = std::env::current_dir()?;

    let mut gen_dir = buildroot.clone();
    gen_dir.push("src/schemas");

    let mut c = CompilerConfig::new(gen_dir, "crate::schemas".to_owned());
    c.module_aliases.insert(vec!["EntityRef".to_owned()], "crate::actor".to_owned());

    let inputs = expand_inputs(&vec!["protocols/schema-bundle.bin".to_owned(),
                                     "local-protocols/schema-bundle.bin".to_owned()])?;
    for i in &inputs {
        println!("cargo:rerun-if-changed={:?}", i);
    }
    c.load_schemas_and_bundles(&inputs)?;
    compile(&c)
}
