use crate::error::DatastoreParseError;

use google_datastore1::schemas::{Entity, EntityResult, Key, Query, Value};

use crate::deserialize::Deserialize;
use crate::serialize::Serialize;
use crate::DatastorersError;
use std::collections::BTreeMap;
use std::convert::From;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};

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

impl Deref for DatastoreValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DatastoreValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<DatastoreValue> for Value {
    fn from(val: DatastoreValue) -> Value {
        val.0
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

    pub fn get<T: Deserialize>(&mut self, key: &str) -> Result<T, DatastorersError> {
        match self.0.remove(key) {
            Some(value) => T::deserialize(DatastoreValue(value)).map_err(|e| e.into()),
            None => match T::default_missing() {
                Some(def) => Ok(def),
                None => Err(DatastoreParseError::NoSuchValue.into()),
            },
        }
    }

    pub fn set<T: Serialize>(&mut self, key: &str, value: T) -> Result<(), DatastorersError> {
        Ok(match value.serialize()? {
            Some(value) => {
                self.0.insert(key.to_string(), value.0);
                ()
            }
            None => (),
        })
    }
}

impl TryFrom<DatastoreEntity> for DatastoreProperties {
    type Error = DatastorersError;

    fn try_from(entity: DatastoreEntity) -> Result<Self, Self::Error> {
        match entity.0.properties {
            Some(properties) => Ok(DatastoreProperties(properties)),
            None => Err(DatastorersError::ParseError(
                DatastoreParseError::NoProperties,
            )),
        }
    }
}

//
// DatastoreEntity
//
#[derive(Debug, Clone)]
pub struct EntityMeta {
    version: Option<i64>,
}

impl Default for EntityMeta {
    fn default() -> Self {
        EntityMeta { version: None }
    }
}

#[derive(Debug, Clone)]
pub struct DatastoreEntity(Entity, EntityMeta);

impl DatastoreEntity {
    pub fn from(
        key: Option<Key>,
        properties: DatastoreProperties,
        version: Option<i64>,
    ) -> DatastoreEntity {
        DatastoreEntity(
            Entity {
                key,
                properties: Some(properties.0),
            },
            EntityMeta { version },
        )
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

    pub fn version(&self) -> Option<i64> {
        self.1.version
    }
}

impl Display for DatastoreEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl TryFrom<EntityResult> for DatastoreEntity {
    type Error = DatastorersError;

    fn try_from(entity_result: EntityResult) -> Result<Self, Self::Error> {
        let version = entity_result.version;
        if let Some(entity) = entity_result.entity {
            let entity_properties = entity.properties.ok_or(DatastorersError::ParseError(
                DatastoreParseError::NoProperties,
            ))?;
            let props = DatastoreProperties::from_map(entity_properties);

            Ok(DatastoreEntity::from(entity.key, props, version))
        } else {
            Err(DatastoreParseError::NoResult)?
        }
    }
}

impl TryFrom<Entity> for DatastoreEntity {
    type Error = DatastorersError;

    fn try_from(entity: Entity) -> Result<Self, Self::Error> {
        let entity_properties = entity.properties.ok_or(DatastorersError::ParseError(
            DatastoreParseError::NoProperties,
        ))?;
        let props = DatastoreProperties::from_map(entity_properties);
        Ok(DatastoreEntity::from(entity.key, props, None))
    }
}

impl TryFrom<DatastoreEntity> for Entity {
    type Error = DatastorersError;

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
        has_more_results: bool,
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
    T: TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    type Error = DatastorersError;

    fn try_from(collection: DatastoreEntityCollection) -> Result<Self, Self::Error> {
        let result_items: Vec<T> = collection
            .entities
            .into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<T>, DatastorersError>>()?;
        Ok(ResultCollection {
            result: result_items,
            query: collection.query,
            end_cursor: collection.end_cursor,
            has_more_results: collection.has_more_results,
        })
    }
}
