use datastore_entity::{DatastoreEntity, DatastoreManaged};
use google_datastore1::schemas::Key;
use std::convert::TryInto;

#[derive(DatastoreManaged, Clone, Debug, Default)]
#[kind = "thingy"]
pub struct Thing {
    #[key]
    pub key_is_good: Option<Key>,
    pub prop_string: String,
    pub prop_integer: i64,
    pub prop_boolean: bool,
    pub prop_str_array: Vec<String>,
}

#[derive(DatastoreManaged, Clone, Debug, Default)]
#[kind = "thingy"]
pub struct VersionedThing {
    #[key]
    pub key_is_good: Option<Key>,

    #[version]
    pub thing_version: Option<i64>,

    pub prop_string: String,
}

#[test]
fn get_generated_properties() {
    let thing = Thing::default();

    assert_eq!("thingy", thing.kind());
    assert_eq!(None, thing.id());
}

#[test]
fn test_version() {
    let test_version = 5345345;
    let versioned_thing = VersionedThing {
        key_is_good: Default::default(),
        thing_version: Some(test_version),
        prop_string: "StrStr".to_string(),
    };

    // From VersionedThing to DatastoreEntity, version shall be included in entity
    let entity: DatastoreEntity = versioned_thing.clone().into();
    assert_eq!(Some(test_version), entity.version());

    // And back again
    let thing_is_back: VersionedThing = entity.clone().try_into().unwrap();
    assert_eq!(Some(test_version), thing_is_back.thing_version);
    assert_eq!("StrStr", thing_is_back.prop_string);
}

#[test]
fn into_datastore_entity_and_back() {
    let thing = Thing {
        key_is_good: Default::default(),
        prop_string: "StrStr".to_string(),
        prop_integer: 777,
        prop_boolean: false,
        prop_str_array: vec![String::from("Str"), String::from("Array")],
    };

    let entity: DatastoreEntity = thing.clone().into();

    let thing_is_back: Thing = entity.clone().try_into().unwrap();

    assert_eq!(thing.prop_string, thing_is_back.prop_string);
    assert_eq!("StrStr", thing_is_back.prop_string);
    assert_eq!(thing.prop_integer, thing_is_back.prop_integer);
    assert_eq!(777, thing_is_back.prop_integer);
    assert_eq!(thing.prop_boolean, thing_is_back.prop_boolean);
    assert_eq!(false, thing_is_back.prop_boolean);
    assert_eq!(
        &vec![String::from("Str"), String::from("Array")],
        &thing.prop_str_array
    );
}
