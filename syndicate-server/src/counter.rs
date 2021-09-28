use std::sync::Arc;

use syndicate::actor::*;

pub fn adjust(t: &mut Activation, f: &Arc<Field<isize>>, delta: isize) {
    let f = f.clone();
    tracing::trace!(v0 = ?t.get(&f), "adjust");
    *t.get_mut(&f) += delta;
    tracing::trace!(v1 = ?t.get(&f), "adjust");
    t.on_stop(move |t| {
        tracing::trace!(v0 = ?t.get(&f), "cleanup");
        *t.get_mut(&f) -= delta;
        tracing::trace!(v1 = ?t.get(&f), "cleanup");
        Ok(())
    });
}

pub fn sync_and_adjust<M: 'static + Send>(t: &mut Activation, r: &Arc<Ref<M>>, f: &Arc<Field<isize>>, delta: isize) {
    let f = f.clone();
    let sync_handler = t.create(move |t: &mut Activation| {
        tracing::trace!(v0 = ?t.get(&f), "sync");
        *t.get_mut(&f) += delta;
        tracing::trace!(v1 = ?t.get(&f), "sync");
        Ok(())
    });
    t.sync(r, sync_handler)
}
