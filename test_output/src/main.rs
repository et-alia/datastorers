
use datastore_entity::{DatastoreEntity};

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
    let mut props = thing.into_properties();
    println!("{}", props);
    let thing2 = Thing::from_properties(&mut props).ok().unwrap();
    println!("{:#?}", thing2);
    println!("{}", thing2.kind());
}
