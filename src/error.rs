pub use super::schemas::internal_protocol::_Any;
pub use super::schemas::internal_protocol::_Ptr;
pub use super::schemas::internal_protocol::Error;

use preserves::value::NestedValue;
use preserves::value::Value;
use preserves_schema::support::ParseError;

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "Error: {}; detail: {:?}", self.message, self.detail)
    }
}

pub fn error<Detail>(message: &str, detail: Detail) -> Error where Value<_Any, _Ptr>: From<Detail> {
    Error {
        message: message.to_owned(),
        detail: _Any::new(detail),
    }
}

impl From<&str> for Error {
    fn from(v: &str) -> Self {
        error(v, false)
    }
}

impl From<std::io::Error> for Error {
    fn from(v: std::io::Error) -> Self {
        error(&format!("{}", v), false)
    }
}

impl From<ParseError> for Error {
    fn from(v: ParseError) -> Self {
        error(&format!("{}", v), false)
    }
}

