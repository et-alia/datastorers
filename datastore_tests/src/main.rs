use datastore_entity::{DatastoreEntity, DatastoreManaged};
use std::convert::TryInto;
use google_datastore1::schemas::Key;

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "thingy"]
pub struct Thing {
    #[key]
    pub key_is_good: Option<Key>,
    pub prop_string: String,
    pub prop_integer: i64,
    pub prop_boolean: bool,
}

#[test]
fn into_datastore_entity_and_back() {
    let thing = Thing {
        key_is_good: Default::default(),
        prop_string: "StrStr".to_string(),
        prop_integer: 777,
        prop_boolean: false,
    };

    assert_eq!("thingy", thing.kind());
    assert_eq!(None, thing.id());

    let entity: DatastoreEntity = thing.clone().into();

    let thing_is_back: Thing = entity.clone().try_into().unwrap();

    assert_eq!(thing.prop_string, thing_is_back.prop_string);
    assert_eq!("StrStr", thing_is_back.prop_string);
    assert_eq!(thing.prop_integer, thing_is_back.prop_integer);
    assert_eq!(777, thing_is_back.prop_integer);
    assert_eq!(thing.prop_boolean, thing_is_back.prop_boolean);
    assert_eq!(false, thing_is_back.prop_boolean);
}

pub fn main() {
    println!("These are not the droids you are looking for")
}