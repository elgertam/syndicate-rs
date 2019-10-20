use super::V;
use super::dataspace;

use preserves::value::Map;

pub struct Spaces {
    index: Map<V, dataspace::DataspaceRef>,
}

impl Spaces {
    pub fn new() -> Self {
        Self { index: Map::new() }
    }

    pub fn lookup(&mut self, name: &V) -> dataspace::DataspaceRef {
        let (is_new, space) = match self.index.get(name) {
            Some(s) => (false, s.clone()),
            None => {
                let s = dataspace::Dataspace::new_ref(name);
                self.index.insert(name.clone(), s.clone());
                (true, s)
            }
        };

        println!("Dataspace {:?} {}",
                 name,
                 if is_new { "created" } else { "accessed" });

        space
    }
}
