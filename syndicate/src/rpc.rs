use preserves_schema::support::Unparse;

use crate::actor::AnyValue;
use crate::schemas::rpc as R;

pub fn question<L, Q: Unparse<L, AnyValue>>(literals: L, request: Q) -> R::Question {
    R::Question {
        request: request.unparse(literals),
    }
}

pub fn answer<'a, L, Q: Unparse<&'a L, AnyValue>, A: Unparse<&'a L, AnyValue>>(literals: &'a L, request: Q, response: A) -> R::Answer {
    R::Answer {
        request: request.unparse(literals),
        response: response.unparse(literals),
    }
}
