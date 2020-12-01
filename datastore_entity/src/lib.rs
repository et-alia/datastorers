use google_datastore1::schemas::{
    Entity, Filter, Key, KindExpression, LookupRequest, LookupResponse, PathElement,
    PropertyFilter, PropertyFilterOp, PropertyReference, Query, RunQueryRequest, RunQueryResponse,
    Value,
};
use google_datastore1::Client;
use std::collections::BTreeMap;

use std::convert::From;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

pub use datastore_entity_derives::DatastoreManaged;

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
}

impl Display for DatastoreEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

pub trait DatastoreFetch<T> {
    fn get_by_id<A>(id: i64, token: A, project_name: &String) -> Result<T, String>
    // TODO - error format
    where
        A: ::google_api_auth::GetAccessToken + 'static;
}

pub fn get_one_by_id<A>(
    id: i64,
    kind: String,
    token: A,
    project_name: &String,
) -> Result<DatastoreEntity, String>
where
    A: ::google_api_auth::GetAccessToken + 'static,
{
    let client = Client::new(token);
    let projects = client.projects();

    let key = Key {
        partition_id: None,
        path: Some(vec![PathElement {
            id: Some(id),
            kind: Some(kind),
            name: None,
        }]),
    };
    let req = LookupRequest {
        keys: Some(vec![key]),
        read_options: None,
    };
    let resp: LookupResponse = projects.lookup(req, project_name).execute().map_err(
        |_e: google_datastore1::Error| -> String { "Failed to fetch entity by id".to_string() },
    )?;

    match resp.found {
        Some(found) => {
            for f in found {
                if let Some(entity) = f.entity {
                    let props = DatastoreProperties::from_map(entity.properties.unwrap());
                    let result = DatastoreEntity::from(entity.key, props);

                    return Ok(result);
                }
            }
            Err("No matching entity found".to_string())
        }
        None => Err("No matching entity found".to_string()),
    }
}

fn get_datastore_value_for_value<K: Into<DatastoreValue>>(value: K) -> Value {
    let datastore_value: DatastoreValue = value.into();
    datastore_value.into()
}

pub fn get_one_by_property<A, K: Into<DatastoreValue>>(
    property_name: String,
    property_value: K,
    kind: String,
    token: A,
    project_name: &String,
) -> Result<DatastoreEntity, String>
where
    A: ::google_api_auth::GetAccessToken + 'static,
{
    let client = Client::new(token);
    let projects = client.projects();

    let mut req = RunQueryRequest::default();
    let mut filter = Filter::default();
    filter.property_filter = Some(PropertyFilter {
        property: Some(PropertyReference {
            name: Some(property_name),
        }),
        value: Some(get_datastore_value_for_value(property_value)),
        op: Some(PropertyFilterOp::Equal),
    });
    let mut query = Query::default();
    query.kind = Some(vec![KindExpression { name: Some(kind) }]);
    query.filter = Some(filter);
    query.limit = Some(1);
    req.query = Some(query);

    let resp: RunQueryResponse = projects.run_query(req, project_name).execute().map_err(
        |_e: google_datastore1::Error| -> String { "Failed to fetch entity by prop".to_string() },
    )?;

    match resp.batch {
        Some(batch) => {
            // TODO - Validate more results -> ther shall not be any more results!
            let found = batch.entity_results.unwrap(); // TODO - check before unwrap
            for f in found {
                // TODO - Validate legth instead of just returning the first element
                if let Some(entity) = f.entity {
                    let props = DatastoreProperties::from_map(entity.properties.unwrap());
                    let result = DatastoreEntity::from(entity.key, props);

                    return Ok(result);
                }
            }
            Err("No matching entity found".to_string())
        }
        None => Err("No matching entity found".to_string()),
    }
}

#[derive(Debug)]
pub enum DatastoreParseError {
    NoSuchValue,
    NoProperties,
}

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
