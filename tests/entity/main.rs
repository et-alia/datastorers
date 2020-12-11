use datastore_entity::{DatastoreEntity, DatastoreManaged};
use std::convert::TryInto;
use google_datastore1::schemas::Key;

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


#[test]
fn get_generated_properties() {
    let thing = Thing::default();

    assert_eq!("thingy", thing.kind());
    assert_eq!(None, thing.id());
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
    assert_eq!(&vec![String::from("Str"), String::from("Array")], &thing.prop_str_array);
}
