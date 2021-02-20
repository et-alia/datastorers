use crate::bytes::Bytes;
use crate::DatastoreValue;
use chrono::{DateTime, NaiveDateTime, ParseError};
use radix64::{DecodeError, STD as BASE64_CFG};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum DatastoreDeserializeError {
    #[error("value not found")]
    NoSuchValue,
    #[error(transparent)]
    Base64DecodeError(#[from] DecodeError),
    #[error(transparent)]
    ParseError(#[from] ParseError),
}

pub trait Deserialize
where
    Self: Sized + Debug,
{
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError>;

    /// If the value is missing from [`datastorers::DatastoreProperties`], provide this default.
    /// The default is None, meaning the missing value is propagated as an error.
    /// This is blanket implemented for Option fields.
    fn default_missing() -> Option<Self> {
        None
    }
}

/// Blanket impl for Option
impl<T: Deserialize> Deserialize for Option<T> {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        match T::deserialize(value) {
            Ok(value) => Ok(Some(value)),
            Err(err) => match err {
                DatastoreDeserializeError::NoSuchValue => Ok(None),
                _ => Err(err),
            },
        }
    }

    fn default_missing() -> Option<Self> {
        Some(None)
    }
}

/// Blanket impl for Vec
impl<T: Deserialize> Deserialize for Vec<T> {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        match value.0.array_value {
            Some(array_value) => array_value.values.map_or_else(
                // If there are no values, just return an empty array
                || Ok(vec![]),
                // If there are values, make sure they are all deserializable
                |array| {
                    array
                        .into_iter()
                        .map(DatastoreValue)
                        .map(T::deserialize)
                        .collect::<Result<Vec<_>, _>>()
                },
            ),
            None => Err(DatastoreDeserializeError::NoSuchValue),
        }
    }
}

impl Deserialize for String {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        value
            .0
            .string_value
            .ok_or(DatastoreDeserializeError::NoSuchValue)
    }
}

impl Deserialize for i64 {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        value
            .0
            .integer_value
            .ok_or(DatastoreDeserializeError::NoSuchValue)
    }
}

impl Deserialize for f64 {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        value
            .0
            .double_value
            .ok_or(DatastoreDeserializeError::NoSuchValue)
    }
}

impl Deserialize for bool {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        value
            .0
            .boolean_value
            .ok_or(DatastoreDeserializeError::NoSuchValue)
    }
}

impl Deserialize for Bytes {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        let blob_b64 = value
            .0
            .blob_value
            .ok_or(DatastoreDeserializeError::NoSuchValue)?;
        let u8_vec = BASE64_CFG.decode(&blob_b64)?;
        Ok(Bytes(u8_vec))
    }
}

impl Deserialize for NaiveDateTime {
    fn deserialize(value: DatastoreValue) -> Result<Self, DatastoreDeserializeError> {
        let date_string = value
            .0
            .timestamp_value
            .ok_or(DatastoreDeserializeError::NoSuchValue)?;
        let date_time = DateTime::parse_from_rfc3339(&date_string)?;
        Ok(date_time.naive_utc())
    }
}
