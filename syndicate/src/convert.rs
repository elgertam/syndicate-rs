//! Useful utilities for working with [`AnyValue`][Actor::AnyValue]s.

use preserves::value::Embeddable;
use preserves::value::IOValue;
use preserves::value::NestedValue;

use std::convert::TryInto;

use crate::actor::*;

/// Converts an `AnyValue` to any [`syndicate::value::NestedValue`],
/// signalling an error if any embedded values are found in `v`.
pub fn from_any_value<N: NestedValue<D>, D: Embeddable>(v: &AnyValue) -> Result<N, &'static str> {
    v.copy_via(&mut |_| Err("Embedded values cannot be converted")?)
}

/// Converts any [`syndicate::value::NestedValue`] to an `AnyValue`,
/// signalling an error if any embedded values are found in `v`.
pub fn to_any_value<N: NestedValue<D>, D: Embeddable>(v: &N) -> Result<AnyValue, &'static str> {
    v.copy_via(&mut |_| Err("Embedded values cannot be converted")?)
}

/// Special case of [`to_any_value`] for [`IOValue`].
pub fn from_io_value<V: TryInto<IOValue>>(v: V) -> Result<AnyValue, &'static str> {
    to_any_value(&v.try_into().map_err(|_| "Could not convert to IOValue")?)
}
