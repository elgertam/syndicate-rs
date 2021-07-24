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

pub fn error<Detail>(message: &str, detail: Detail) -> Error where _Any: From<Detail> {
    Error {
        message: message.to_owned(),
        detail: _Any::from(detail),
    }
}

pub fn encode_error(result: Result<(), Error>) -> _Any {
    match result {
        Ok(()) => {
            let mut r = Value::record(Value::symbol("Ok").wrap(), 1);
            r.fields_vec_mut().push(Value::record(Value::symbol("tuple").wrap(), 0).finish().wrap());
            r.finish().wrap()
        }
        Err(e) => {
            let mut r = Value::record(Value::symbol("Err").wrap(), 1);
            r.fields_vec_mut().push((&e).into());
            r.finish().wrap()
        }
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
