use crate::bytes::Bytes;
use crate::DatastoreValue;
use chrono::naive::NaiveDateTime;
use chrono::{DateTime, LocalResult, SecondsFormat, TimeZone, Utc};
use google_datastore1::schemas::ArrayValue;
use radix64::STD as BASE64_CFG;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum DatastoreSerializeError {
    #[error("Could not serialize timestamp_value")]
    DateTimeError,
    #[error("A non empty value must be set")]
    NoValueError,
}

pub trait Serialize {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError>;
}

/// Blanket impl for Option
impl<T: Serialize> Serialize for Option<T> {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        match self {
            Some(some) => some.serialize(),
            None => Ok(None),
        }
    }
}

/// Blanket impl for Vec
impl<T: Serialize> Serialize for Vec<T> {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let array_of_opts = self
            .into_iter()
            .map(Serialize::serialize)
            .collect::<Result<Vec<_>, _>>()?;
        let array = array_of_opts.into_iter().flatten().map(|v| v.0).collect();
        let mut value = DatastoreValue::empty();
        value.array_value = Some(ArrayValue {
            values: Some(array),
        });
        Ok(Some(value))
    }
}

impl Serialize for String {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let mut value = DatastoreValue::empty();
        value.string_value = Some(self);
        Ok(Some(value))
    }
}

impl Serialize for i64 {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let mut value = DatastoreValue::empty();
        value.integer_value = Some(self);
        Ok(Some(value))
    }
}

impl Serialize for f64 {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let mut value = DatastoreValue::empty();
        value.double_value = Some(self);
        Ok(Some(value))
    }
}

impl Serialize for bool {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let mut value = DatastoreValue::empty();
        value.boolean_value = Some(self);
        Ok(Some(value))
    }
}

impl Serialize for Bytes {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let mut value = DatastoreValue::empty();
        let encoded = BASE64_CFG.encode(&self.0);
        value.blob_value = Some(encoded);
        Ok(Some(value))
    }
}

impl Serialize for NaiveDateTime {
    fn serialize(self) -> Result<Option<DatastoreValue>, DatastoreSerializeError> {
        let mut value = DatastoreValue::empty();
        let date_time: DateTime<Utc> = match Utc.from_local_datetime(&self) {
            LocalResult::None => return Err(DatastoreSerializeError::DateTimeError),
            LocalResult::Single(date_time) => date_time,
            LocalResult::Ambiguous(_from, to) => {
                // In UTC, `from` and `to` should be equal to each other (no DST, etc.), so just pick `to`
                to
            }
        };
        // Datastore requires timestamp_value to have at most 3 decimals (millis) for seconds.
        value.timestamp_value = Some(date_time.to_rfc3339_opts(SecondsFormat::Millis, true));
        Ok(Some(value))
    }
}
