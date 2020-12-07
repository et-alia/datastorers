use google_datastore1::schemas::{Entity, Key, Value};
use thiserror::Error;

use std::collections::BTreeMap;
use std::convert::From;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

//
// DatastoreEntity related errors
//
#[derive(Error, Debug)]
pub enum DatastoreParseError {
    #[error("value not found")]
    NoSuchValue,
    #[error("no properties found on entity")]
    NoProperties,
}

//
// DatastoreValue
//
pub struct DatastoreValue(pub Value);

impl Default for DatastoreValue {
    fn default() -> Self {
        DatastoreValue(Value {
            array_value: None,
            blob_value: None,
            boolean_value: None,
            double_value: None,
            entity_value: None,
            exclude_from_indexes: None,
            geo_point_value: None,
            integer_value: None,
            key_value: None,
            meaning: None,
            null_value: None,
            string_value: None,
            timestamp_value: None,
        })
    }
}

impl DatastoreValue {
    pub fn string(&mut self, s: String) {
        self.0.string_value = Some(s);
    }

    pub fn integer(&mut self, i: i64) {
        self.0.integer_value = Some(i);
    }

    pub fn boolean(&mut self, b: bool) {
        self.0.boolean_value = Some(b);
    }
}

impl Deref for DatastoreValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DatastoreValue> for Value {
    fn from(val: DatastoreValue) -> Value {
        val.0
    }
}

impl From<String> for DatastoreValue {
    fn from(string_value: String) -> DatastoreValue {
        let mut val = DatastoreValue::default();
        val.string(string_value);
        return val;
    }
}

impl From<bool> for DatastoreValue {
    fn from(bool_value: bool) -> DatastoreValue {
        let mut val = DatastoreValue::default();
        val.boolean(bool_value);
        return val;
    }
}

impl From<i64> for DatastoreValue {
    fn from(int_value: i64) -> DatastoreValue {
        let mut val = DatastoreValue::default();
        val.integer(int_value);
        return val;
    }
}

//
// DatastoreProperties
//
#[derive(Debug)]
pub struct DatastoreProperties(BTreeMap<String, Value>);

impl Display for DatastoreProperties {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl DatastoreProperties {
    pub fn new() -> DatastoreProperties {
        DatastoreProperties(BTreeMap::<String, Value>::new())
    }

    pub fn from(entity: DatastoreEntity) -> Option<DatastoreProperties> {
        Some(DatastoreProperties(entity.0.properties?))
    }

    pub fn from_map(map: BTreeMap<String, Value>) -> DatastoreProperties {
        DatastoreProperties(map)
    }

    pub fn into_map(self) -> BTreeMap<String, Value> {
        return self.0;
    }

    pub fn get_string(&mut self, key: &str) -> Result<String, DatastoreParseError> {
        match self.0.remove(key) {
            Some(value) => value
                .string_value
                .ok_or_else(|| DatastoreParseError::NoSuchValue),
            None => Err(DatastoreParseError::NoSuchValue),
        }
    }

    pub fn set_opt_string(&mut self, key: &str, value: Option<String>) {
        let mut datastore_value = DatastoreValue::default();
        if let Some(value) = value {
            datastore_value.string(value.to_string());
            self.0.insert(key.to_string(), datastore_value.0);
        }
    }

    pub fn set_string(&mut self, key: &str, value: String) {
        let mut datastore_value = DatastoreValue::default();
        datastore_value.string(value);
        self.0.insert(key.to_string(), datastore_value.0);
    }

    pub fn get_integer(&mut self, key: &str) -> Result<i64, DatastoreParseError> {
        match self.0.remove(key) {
            Some(value) => value
                .integer_value
                .ok_or_else(|| DatastoreParseError::NoSuchValue),
            None => Err(DatastoreParseError::NoSuchValue),
        }
    }

    pub fn set_integer(&mut self, key: &str, value: i64) {
        let mut datastore_value = DatastoreValue::default();
        datastore_value.integer(value);
        self.0.insert(key.to_string(), datastore_value.0);
    }

    pub fn get_bool(&mut self, key: &str) -> Result<bool, DatastoreParseError> {
        match self.0.remove(key) {
            Some(value) => value
                .boolean_value
                .ok_or_else(|| DatastoreParseError::NoSuchValue),
            None => Err(DatastoreParseError::NoSuchValue),
        }
    }

    pub fn set_bool(&mut self, key: &str, value: bool) {
        let mut datastore_value = DatastoreValue::default();
        datastore_value.boolean(value);
        self.0.insert(key.to_string(), datastore_value.0);
    }
}

//
// DatastoreEntity
//
#[derive(Debug, Clone)]
pub struct DatastoreEntity(Entity);

impl DatastoreEntity {
    pub fn from(key: Option<Key>, properties: DatastoreProperties) -> DatastoreEntity {
        DatastoreEntity(Entity {
            key,
            properties: Some(properties.0),
        })
    }

    pub fn key(&self) -> Option<Key> {
        self.0.key.clone()
    }

    pub fn has_key(&self) -> bool {
        self.0.key.is_some()
    }

    pub fn set_key(&mut self, key: Option<Key>) {
        self.0.key = key;
    }
}

impl Display for DatastoreEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}
