use chrono::{NaiveDateTime, Utc};
use datastorers::deserialize::Deserialize;
use datastorers::serialize::Serialize;
use datastorers::{DatastoreEntity, DatastoreManaged, DatastoreValue, DatastorersError};
use float_cmp::approx_eq;
use google_datastore1::schemas::Key;
use std::convert::TryInto;
use std::error::Error;

#[derive(Debug)]
pub enum TestError {
    Unknown,
}

impl Error for TestError {}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Unknown test error")
    }
}

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "thingy"]
pub struct Thing {
    #[key]
    pub key_is_good: Option<Key>,
    pub prop_string: String,
    pub prop_integer: i64,
    pub prop_double: f64,
    pub prop_boolean: bool,
    pub prop_str_array: Vec<String>,
    pub prop_date: NaiveDateTime,
}

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "thingy"]
pub struct VersionedThing {
    #[key]
    pub key_is_good: Option<Key>,

    #[version]
    pub thing_version: Option<i64>,

    pub prop_string: String,
}

fn datastore_timestamp_now() -> NaiveDateTime {
    let now = Utc::now().naive_utc();
    // Make `now` into datastore accepted format string wrapped in a DatastoreValue
    let value = now.serialize().unwrap().unwrap();
    // Convert the DatastoreValue back to a NaiveDateTime
    NaiveDateTime::deserialize(value).unwrap()
}

#[test]
fn test_version() -> Result<(), DatastorersError> {
    let test_version = 5345345;
    let versioned_thing = VersionedThing {
        key_is_good: Default::default(),
        thing_version: Some(test_version),
        prop_string: "StrStr".to_string(),
    };

    // From VersionedThing to DatastoreEntity, version shall be included in entity
    let entity: DatastoreEntity = versioned_thing.try_into()?;
    assert_eq!(Some(test_version), entity.version());

    // And back again
    let thing_is_back: VersionedThing = entity.try_into().unwrap();
    assert_eq!(Some(test_version), thing_is_back.thing_version);
    assert_eq!("StrStr", thing_is_back.prop_string);
    Ok(())
}

#[test]
fn into_datastore_entity_and_back() -> Result<(), DatastorersError> {
    let now = datastore_timestamp_now();
    let thing = Thing {
        key_is_good: Default::default(),
        prop_string: "StrStr".to_string(),
        prop_integer: 777,
        prop_double: 987.12,
        prop_boolean: false,
        prop_str_array: vec![String::from("Str"), String::from("Array")],
        prop_date: now,
    };

    assert_eq!("thingy", thing.kind());
    assert_eq!(None, thing.id());

    let entity: DatastoreEntity = thing.clone().try_into()?;

    let thing_is_back: Thing = entity.try_into().unwrap();

    assert_eq!(thing.prop_string, thing_is_back.prop_string);
    assert_eq!("StrStr", thing_is_back.prop_string);
    assert_eq!(thing.prop_integer, thing_is_back.prop_integer);
    assert_eq!(777, thing_is_back.prop_integer);
    assert!(approx_eq!(
        f64,
        thing.prop_double,
        thing_is_back.prop_double
    ));
    assert!(approx_eq!(f64, 987.12, thing_is_back.prop_double));
    assert_eq!(thing.prop_boolean, thing_is_back.prop_boolean);
    assert_eq!(false, thing_is_back.prop_boolean);
    assert_eq!(
        &vec![String::from("Str"), String::from("Array")],
        &thing.prop_str_array
    );
    assert_eq!(thing.prop_date, thing_is_back.prop_date);
    assert_eq!(now, thing_is_back.prop_date);
    Ok(())
}

#[test]
fn timestap_deserialize_serialize() -> Result<(), Box<dyn Error>> {
    let date_str = "2021-01-02T12:53:39.392Z";
    let mut value_to_deserialize = DatastoreValue::empty();
    value_to_deserialize.timestamp_value = Some(date_str.to_string());
    let date_time = NaiveDateTime::deserialize(value_to_deserialize)?;
    let serialized = date_time.serialize()?;
    let serialized_value: String =
        get_datastore_value(&serialized, |d| d.0.timestamp_value.as_ref())?;
    assert_eq!(date_str, serialized_value);
    Ok(())
}

fn get_datastore_value<T, F>(
    datastore_value: &Option<DatastoreValue>,
    select_prop: F,
) -> Result<T, Box<dyn Error>>
where
    T: Clone,
    F: for<'a> Fn(&'a DatastoreValue) -> Option<&'a T>,
{
    match datastore_value {
        None => Err(Box::new(TestError::Unknown)),
        Some(serialized_value) => {
            let selected_prop = select_prop(&serialized_value);
            match selected_prop {
                None => Err(Box::new(TestError::Unknown)),
                Some(serialized_value_2) => Ok(serialized_value_2.clone()),
            }
        }
    }
}
