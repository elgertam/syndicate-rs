//! Actor errors.

use super::actor::AnyValue;
use super::language;
use super::schemas::protocol as P;

use preserves::value::NestedValue;
use preserves::value::Value;
use preserves_schema::Codec;
use preserves_schema::ParseError;

pub type Error = P::Error<AnyValue>;

impl Error {
    /// Construct an [`Error`] given a displayable `T`.
    /// Uses the displayed form of the `T` as `message`, and `false` as detail (per convention).
    ///
    /// This function is useful to translate [`std::error::Error`] from some other API
    /// into Syndicate `Error`:
    ///
    /// ```
    /// return some_operation().map_err(Error::msg)
    /// ```
    pub fn msg<T: std::fmt::Display>(e: T) -> Self {
        error(&e.to_string(), AnyValue::new(false))
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "Error: {}; detail: {:?}", self.message, self.detail)
    }
}

/// Construct an [`Error`] with the given `message` and `detail`; see also [`Error::msg`].
///
/// When no relevant detail exists, convention is to set `detail` to `false`.
pub fn error<Detail>(message: &str, detail: Detail) -> Error where AnyValue: From<Detail> {
    Error {
        message: message.to_owned(),
        detail: AnyValue::from(detail),
    }
}

/// Encodes an [`ActorResult`][crate::actor::ActorResult] as an
/// [`AnyValue`][crate::actor::AnyValue].
///
/// Used primarily when attempting to perform an
/// [`Activation`][crate::actor::Activation] on an already-terminated
/// actor.
pub fn encode_error(result: Result<(), Error>) -> AnyValue {
    match result {
        Ok(()) => {
            let mut r = Value::record(AnyValue::symbol("Ok"), 1);
            r.fields_vec_mut().push(Value::record(AnyValue::symbol("tuple"), 0).finish().wrap());
            r.finish().wrap()
        }
        Err(e) => {
            let mut r = Value::record(AnyValue::symbol("Err"), 1);
            r.fields_vec_mut().push(language().unparse(&e));
            r.finish().wrap()
        }
    }
}

impl<'a> From<&'a str> for Error {
    fn from(v: &'a str) -> Self {
        error(v, AnyValue::new(false))
    }
}

impl From<std::io::Error> for Error {
    fn from(v: std::io::Error) -> Self {
        error(&format!("{}", v), AnyValue::new(false))
    }
}

impl From<ParseError> for Error {
    fn from(v: ParseError) -> Self {
        error(&format!("{}", v), AnyValue::new(false))
    }
}

impl From<preserves::error::Error> for Error {
    fn from(v: preserves::error::Error) -> Self {
        error(&format!("{}", v), AnyValue::new(false))
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(v: Box<dyn std::error::Error>) -> Self {
        match v.downcast::<Error>() {
            Ok(e) => *e,
            Err(v) => error(&format!("{}", v), AnyValue::new(false)),
        }
    }
}

impl From<Box<dyn std::error::Error + Send + Sync + 'static>> for Error {
    fn from(v: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
        match v.downcast::<Error>() {
            Ok(e) => *e,
            Err(v) => error(&format!("{}", v), AnyValue::new(false)),
        }
    }
}
