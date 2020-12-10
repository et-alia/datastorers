use google_datastore1::schemas::{ArrayValue, Entity, Key, Value, Query};
use thiserror::Error;

use std::collections::BTreeMap;
use std::convert::From;
use std::convert::TryFrom;
use std::convert::TryInto;
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
    #[error("unexpected type in array item")]
    InvalidArrayValueFormat,
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

    pub fn array(&mut self, a: Vec<Value>) {
        self.0.array_value = Some(ArrayValue { values: Some(a) });
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

impl From<Vec<String>> for DatastoreValue {
    fn from(str_vec_value: Vec<String>) -> DatastoreValue {
        fn generate_string_value(str_val: String) -> Value {
            let mut val = DatastoreValue::default();
            val.string(str_val);
            val.0
        }
        let mut array_val = DatastoreValue::default();
        array_val.array(
            str_vec_value
                .into_iter()
                .map(|s| generate_string_value(s))
                .collect(),
        );
        return array_val;
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

    pub fn get_string_array(&mut self, key: &str) -> Result<Vec<String>, DatastoreParseError> {
        match self.0.remove(key) {
            Some(value) => match value.array_value {
                Some(array_value) => match array_value.values {
                    Some(values) => values
                        .into_iter()
                        .map(|v| {
                            v.string_value
                                .ok_or_else(|| DatastoreParseError::InvalidArrayValueFormat)
                        })
                        .collect(),
                    None => Ok(vec![]), // Empty array
                },
                None => Err(DatastoreParseError::NoSuchValue),
            },
            None => Err(DatastoreParseError::NoSuchValue),
        }
    }

    pub fn set_string_array(&mut self, key: &str, value: Vec<String>) {
        let datastore_value: DatastoreValue = value.into();
        self.0.insert(key.to_string(), datastore_value.0);
    }
}

impl TryFrom<DatastoreEntity> for DatastoreProperties {
    type Error = DatastoreParseError;

    fn try_from(entity: DatastoreEntity) -> Result<Self, Self::Error> {
        match entity.0.properties {
            Some(properties) => Ok(DatastoreProperties(properties)),
            None => Err(DatastoreParseError::NoProperties),
        }
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

impl TryFrom<Entity> for DatastoreEntity {
    type Error = DatastoreParseError;

    fn try_from(entity: Entity) -> Result<Self, Self::Error> {
        let entity_properties = entity.properties.ok_or(DatastoreParseError::NoProperties)?;
        let props = DatastoreProperties::from_map(entity_properties);
        Ok(DatastoreEntity::from(entity.key, props))
    }
}

impl TryFrom<DatastoreEntity> for Entity {
    type Error = DatastoreParseError;

    fn try_from(entity: DatastoreEntity) -> Result<Self, Self::Error> {
        let key = entity.key();
        let properties: DatastoreProperties = entity.try_into()?;

        Ok(Entity {
            key: key,
            properties: Some(properties.into_map()),
        })
    }
}

//
// Datastore entity collections are used to wrap query results that may be paged
//
pub struct DatastoreEntityCollection {
    entities: Vec<DatastoreEntity>,
    query: Option<Query>,
    end_cursor: Option<String>,
    has_more_results: bool,
}


impl Default for DatastoreEntityCollection {
    fn default() -> Self {
        DatastoreEntityCollection {
            entities: vec![],
            query: None,
            end_cursor: None,
            has_more_results: false,
        }
    }
}



impl DatastoreEntityCollection {
    pub fn from_result(
        entities: Vec<DatastoreEntity>,
        query: Query,
        end_cursor: String,
        has_more_results: bool
    ) -> DatastoreEntityCollection {
        DatastoreEntityCollection {
            entities,
            query: Some(query),
            end_cursor: Some(end_cursor),
            has_more_results,
        }
    }
}

#[derive(Debug)]
pub struct ResultCollection<T> {
    pub result: Vec<T>,
    pub query: Option<Query>,
    pub end_cursor: Option<String>,
    pub has_more_results: bool,
}

impl<T> TryFrom<DatastoreEntityCollection> for ResultCollection<T> 
where
    T: TryFrom<DatastoreEntity, Error = DatastoreParseError>
{
    type Error = DatastoreParseError;

    fn try_from(collection: DatastoreEntityCollection) -> Result<Self, Self::Error> {
        let result_items: Vec<T> = collection.entities.into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<T>, DatastoreParseError>>()?;
        Ok(ResultCollection {
            result: result_items,
            query: collection.query,
            end_cursor: collection.end_cursor,
            has_more_results: collection.has_more_results,
        })
    }
}