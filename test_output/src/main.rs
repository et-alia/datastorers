
use datastore_entity::{DatastoreEntity, DatastoreProperties};
use std::convert::TryInto;
use std::convert::From;

#[derive(DatastoreEntity, Debug)]
pub struct Thing {
    pub prop_string: String,
    pub prop_integer: i64,
    pub prop_boolean: bool,
}

impl DatastoreEntity for Thing {
    fn kind(&self) -> &'static str {
        "Thing"
    }
}

pub fn main() {
    let thing = Thing {
        prop_string: "StrStr".to_string(),
        prop_integer: 777,
        prop_boolean: false,
    };
    let props: DatastoreProperties = thing.into();
    // OR: let props = DatastoreProperties::from(thing);
    println!("{}", props);
    let thing2: Thing = props.try_into().unwrap();
    // OR: let thing2 = Thing::try_from(props).unwrap();
    println!("{:#?}", thing2);
    println!("{}", thing2.kind());
}
