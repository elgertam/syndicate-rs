use std::sync::Arc;

use syndicate::actor::*;

pub fn adjust(t: &mut Activation, f: &Arc<Field<isize>>, delta: isize) {
    let f = f.clone();
    *t.get_mut(&f) += delta;
    t.on_stop(move |t| {
        *t.get_mut(&f) -= delta;
        Ok(())
    });
}

pub fn sync_and_adjust<M: 'static + Send>(t: &mut Activation, r: &Arc<Ref<M>>, f: &Arc<Field<isize>>, delta: isize) {
    let f = f.clone();
    let sync_handler = t.create(move |t: &mut Activation| {
        *t.get_mut(&f) += delta;
        Ok(())
    });
    t.sync(r, sync_handler)
}
