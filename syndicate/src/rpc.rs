use preserves_schema::support::Unparse;

use std::sync::Arc;
use crate::actor::Cap;
use crate::schemas::rpc as R;

pub fn question<L, Q: Unparse<L, Arc<Cap>>>(literals: L, request: Q) -> R::Question {
    R::Question {
        request: request.unparse(literals),
    }
}

pub fn answer<'a, L, Q: Unparse<&'a L, Arc<Cap>>, A: Unparse<&'a L, Arc<Cap>>>(literals: &'a L, request: Q, response: A) -> R::Answer {
    R::Answer {
        request: request.unparse(literals),
        response: response.unparse(literals),
    }
}
