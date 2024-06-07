use preserves_schema::support::Codec;
use preserves_schema::support::Unparse;

use crate::actor::AnyValue;
use crate::language;
use crate::schemas::rpc as R;

pub fn answer<'a, L, Q: Unparse<&'a L, AnyValue>, A: Unparse<&'a L, AnyValue>>(literals: &'a L, request: Q, response: A) -> R::Answer {
    R::Answer {
        request: request.unparse(literals),
        response: response.unparse(literals),
    }
}

pub fn answer_value<'a, L, Q: Unparse<&'a L, AnyValue>, A: Unparse<&'a L, AnyValue>>(literals: &'a L, request: Q, response: A) -> AnyValue {
    language().unparse(&answer(literals, request, response))
}
