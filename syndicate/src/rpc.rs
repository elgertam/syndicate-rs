use preserves_schema::Unparse;

use std::sync::Arc;
use crate::actor::Cap;
use crate::schemas::rpc as R;

pub fn question<Q: Unparse<Arc<Cap>>>(request: Q) -> R::Question {
    R::Question {
        request: request.unparse(),
    }
}

pub fn answer<Q: Unparse<Arc<Cap>>, A: Unparse<Arc<Cap>>>(request: Q, response: A) -> R::Answer {
    R::Answer {
        request: request.unparse(),
        response: response.unparse(),
    }
}
