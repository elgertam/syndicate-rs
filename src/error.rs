pub use super::schemas::internal_protocol::_Any;
pub use super::schemas::internal_protocol::_Ptr;
pub use super::schemas::internal_protocol::Error;

use preserves::value::NestedValue;
use preserves_schema::support::ParseError;

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "Error: {}; detail: {:?}", self.message, self.detail)
    }
}

pub fn error<Detail>(message: &str, detail: Detail) -> Error where _Any: From<Detail> {
    Error {
        message: message.to_owned(),
        detail: detail.into(),
    }
}

impl From<&str> for Error {
    fn from(v: &str) -> Self {
        error(v, _Any::new(false))
    }
}

impl From<std::io::Error> for Error {
    fn from(v: std::io::Error) -> Self {
        error(&format!("{}", v), _Any::new(false))
    }
}

impl From<ParseError> for Error {
    fn from(v: ParseError) -> Self {
        error(&format!("{}", v), _Any::new(false))
    }
}

impl From<preserves::error::Error> for Error {
    fn from(v: preserves::error::Error) -> Self {
        error(&format!("{}", v), _Any::new(false))
    }
}
