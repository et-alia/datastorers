use datastorers::{
    id, name, DatastoreEntity, DatastoreKeyError, DatastoreManaged,
    DatastoreNameRepresentationError, DatastoreProperties, DatastorersError,
    DeserializeIdentifierName, IdentifierId, IdentifierName, IdentifierNone, IdentifierString,
    KeyPath, KeyPathElement, Kind, SerializeIdentifierName,
};
use google_datastore1::schemas::{Key, PathElement};
use std::convert::TryInto;

#[derive(Debug)]
struct KindA {}

impl Kind for KindA {
    fn kind(&self) -> &'static str {
        "a"
    }

    fn kind_str() -> &'static str {
        "a"
    }
}

#[derive(Debug)]
struct KindB {}

impl Kind for KindB {
    fn kind(&self) -> &'static str {
        "b"
    }

    fn kind_str() -> &'static str {
        "b"
    }
}

#[derive(Debug)]
struct KindC {}

impl Kind for KindC {
    fn kind(&self) -> &'static str {
        "c"
    }

    fn kind_str() -> &'static str {
        "c"
    }
}

fn get_key_path_error<T>(result: Result<T, DatastorersError>) -> DatastoreKeyError {
    match result {
        Ok(_) => panic!("Unexpected OK"),
        Err(e) => match e {
            DatastorersError::DatastoreKeyError(e) => e,
            _ => panic!("Unexpected err {:?}", e),
        },
    }
}

#[test]
fn test_identifier_id() {
    let identifier = IdentifierId::<KindA>::id(Some(1000), IdentifierNone::none());
    let key = identifier.get_key();
    let key_path = key.path.unwrap();
    assert_eq!(1, key_path.len());
    assert_eq!(1000, key_path[0].id.unwrap());
    assert_eq!("a", key_path[0].kind.as_ref().unwrap());
}

#[test]
fn test_identifier_name() {
    let identifier =
        IdentifierString::<KindB>::name(Some("xyz".to_string()), IdentifierNone::none());
    let key = identifier.get_key();
    let key_path = key.path.unwrap();
    assert_eq!(1, key_path.len());
    assert_eq!("xyz", key_path[0].name.as_ref().unwrap());
    assert_eq!("b", key_path[0].kind.as_ref().unwrap());
}

#[test]
fn test_identifier_id_then_name() {
    let identifier: IdentifierId<KindA, IdentifierString<KindB>> = IdentifierId::id(
        Some(92874),
        IdentifierString::name(Some("bla".to_string()), IdentifierNone::none()),
    );
    let key = identifier.get_key();
    let key_path = key.path.unwrap();
    assert_eq!(2, key_path.len());
    assert_eq!(92874, key_path[0].id.unwrap());
    assert_eq!("a", key_path[0].kind.as_ref().unwrap());
    assert_eq!("bla", key_path[1].name.as_ref().unwrap());
    assert_eq!("b", key_path[1].kind.as_ref().unwrap());
}

#[test]
fn test_identifier_name_then_id_then_name() {
    let identifier: IdentifierString<KindA, IdentifierId<KindB, IdentifierString<KindC>>> =
        IdentifierString::name(
            Some("foo".to_string()),
            IdentifierId::id(
                Some(543),
                IdentifierString::name(Some("bla".to_string()), IdentifierNone::none()),
            ),
        );
    let key = identifier.get_key();
    let key_path = key.path.unwrap();
    assert_eq!(3, key_path.len());
    assert_eq!("foo", key_path[0].name.as_ref().unwrap());
    assert_eq!("a", key_path[0].kind.as_ref().unwrap());
    assert_eq!(543, key_path[1].id.unwrap());
    assert_eq!("b", key_path[1].kind.as_ref().unwrap());
    assert_eq!("bla", key_path[2].name.as_ref().unwrap());
    assert_eq!("c", key_path[2].kind.as_ref().unwrap());
}

#[test]
fn deserialize_from_valid_incomplete_key() -> Result<(), DatastorersError> {
    let key = Key {
        partition_id: None,
        path: Some(vec![
            PathElement {
                id: None,
                kind: Some("a".to_string()),
                name: Some("ancestor".to_string()),
            },
            PathElement {
                id: None,
                kind: Some("b".to_string()),
                name: None,
            },
        ]),
    };

    let identifier: IdentifierString<KindA, IdentifierId<KindB>> = key.try_into()?;

    assert_eq!("a", identifier.kind());
    assert_eq!(Some("ancestor".to_string()), identifier.name);
    assert_eq!(None, identifier.child.id);
    assert_eq!("b", identifier.child.kind());

    Ok(())
}

#[test]
fn deserialize_from_invalid_key_without_name() {
    let key = Key {
        partition_id: None,
        path: Some(vec![
            PathElement {
                id: Some(487),
                kind: Some("a".to_string()),
                name: None,
            },
            PathElement {
                id: None,
                kind: Some("b".to_string()),
                name: None,
            },
        ]),
    };

    let identifier: Result<IdentifierId<KindA, IdentifierString<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(DatastoreKeyError::ExpectedName, error);
}

#[test]
fn deserialize_from_invalid_key_without_id() {
    let key = Key {
        partition_id: None,
        path: Some(vec![
            PathElement {
                id: None,
                kind: Some("a".to_string()),
                name: None,
            },
            PathElement {
                id: None,
                kind: Some("b".to_string()),
                name: None,
            },
        ]),
    };

    let identifier: Result<IdentifierId<KindA, IdentifierId<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(DatastoreKeyError::ExpectedId, error);
}

#[test]
fn deserialize_from_invalid_key_with_wrong_kind() {
    let key = Key {
        partition_id: None,
        path: Some(vec![
            PathElement {
                id: Some(487),
                kind: Some("a".to_string()),
                name: None,
            },
            PathElement {
                id: Some(249),
                kind: Some("c".to_string()),
                name: None,
            },
        ]),
    };

    let identifier: Result<IdentifierId<KindA, IdentifierId<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(
        DatastoreKeyError::WrongKind {
            expected: "b",
            found: "c".to_string()
        },
        error
    );
}

#[test]
fn deserialize_from_invalid_key_with_no_kind() {
    let key = Key {
        partition_id: None,
        path: Some(vec![
            PathElement {
                id: Some(487),
                kind: Some("a".to_string()),
                name: None,
            },
            PathElement {
                id: Some(249),
                kind: None,
                name: None,
            },
        ]),
    };

    let identifier: Result<IdentifierId<KindA, IdentifierId<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(DatastoreKeyError::NoKind, error);
}

#[test]
fn deserialize_from_invalid_key_with_no_key_path_elements() {
    let key = Key {
        partition_id: None,
        path: Some(vec![]),
    };

    let identifier: Result<IdentifierId<KindA, IdentifierId<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(DatastoreKeyError::NoKeyPathElement, error);
}

#[test]
fn deserialize_from_invalid_key_with_no_key_path() {
    let key = Key {
        partition_id: None,
        path: None,
    };

    let identifier: Result<IdentifierId<KindA, IdentifierId<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(DatastoreKeyError::NoKeyPath, error);
}

#[test]
fn deserialize_from_invalid_key_with_no_key() {
    let key = Key {
        partition_id: None,
        path: None,
    };

    let identifier: Result<IdentifierId<KindA, IdentifierId<KindB>>, DatastorersError> =
        key.try_into();

    let error = get_key_path_error(identifier);

    assert_eq!(DatastoreKeyError::NoKeyPath, error);
}

#[test]
fn deserialize_from_entity_without_key() {
    #[derive(DatastoreManaged, Clone, Debug)]
    #[kind = "dummy"]
    pub struct Dummy {
        #[key]
        pub key: IdentifierId<Self>,
    }

    // Create an entity without a Key
    let entity = DatastoreEntity::from(None, DatastoreProperties::new(), None);
    // Try to deserialize it into our Dummy struct
    let test: Result<Dummy, DatastorersError> = entity.try_into();

    let error = get_key_path_error(test);

    // We couldn't find a key at all, which is an error
    assert_eq!(DatastoreKeyError::NoKey, error);
}

#[test]
fn test_id_macro() {
    let identifier: IdentifierId<KindA> = id![1001];
    assert_eq!(1001, identifier.id.unwrap());
}

#[test]
fn test_id_macro_none() {
    let identifier: IdentifierId<KindA> = id![None];
    assert_eq!(None, identifier.id);
}

#[test]
fn test_id_macro_variable() {
    let id = 510;
    let identifier: IdentifierId<KindA> = id![id];
    assert_eq!(510, identifier.id.unwrap());
}

#[test]
fn test_name_macro() {
    let identifier: IdentifierString<KindB> = name!["name"];
    assert_eq!("name", identifier.name.unwrap());
}

#[test]
fn test_name_macro_none() {
    let identifier: IdentifierString<KindB> = name![None];
    assert_eq!(None, identifier.name);
}

#[test]
fn test_name_macro_variable() {
    let s = "thing".to_string();
    let identifier: IdentifierString<KindB> = name![s];
    assert_eq!(s, identifier.name.unwrap());
}

#[test]
fn test_id_macro_name_macro() {
    let identifier: IdentifierId<KindA, IdentifierString<KindB>> = id![2001, name!["thing"]];
    assert_eq!(2001, identifier.id.unwrap());
    assert_eq!("thing", identifier.child.name.unwrap());
}

#[test]
fn test_id_macro_none_name_macro() {
    let identifier: IdentifierId<KindA, IdentifierString<KindB>> = id![None, name!["thing"]];
    assert_eq!(None, identifier.id);
    assert_eq!("thing", identifier.child.name.unwrap());
}

#[test]
fn test_name_macro_none_name_macro() {
    let identifier: IdentifierString<KindA, IdentifierString<KindB>> = name![None, name!["thing"]];
    assert_eq!(None, identifier.name);
    assert_eq!("thing", identifier.child.name.unwrap());
}

#[test]
fn test_name_macro_with_representation() {
    #[derive(Clone, PartialEq)]
    struct Repr {
        x: i32,
    }

    impl SerializeIdentifierName for Repr {
        fn to_string(&self) -> String {
            format!("{}", self.x)
        }
    }

    impl DeserializeIdentifierName for Repr {
        fn from_str(from: &str) -> Result<Self, DatastoreNameRepresentationError> {
            Ok(Repr {
                x: from.parse::<i32>().unwrap(),
            })
        }
    }

    type IdentifierRepr<T, Child = IdentifierNone> = IdentifierName<T, Repr, Child>;

    let identifier: IdentifierRepr<KindA, IdentifierNone> = name![Repr { x: 5 }];
    assert!(Repr { x: 5 } == identifier.name.unwrap());
}

#[test]
fn test_key_deserialization_with_representation() -> Result<(), DatastorersError> {
    #[derive(Clone, PartialEq)]
    struct Repr {
        y: String,
    }

    impl SerializeIdentifierName for Repr {
        fn to_string(&self) -> String {
            self.y.clone()
        }
    }

    impl DeserializeIdentifierName for Repr {
        fn from_str(from: &str) -> Result<Self, DatastoreNameRepresentationError> {
            Ok(Repr {
                y: from.to_string(),
            })
        }
    }

    type IdentifierRepr<T, Child = IdentifierNone> = IdentifierName<T, Repr, Child>;

    let key = Key {
        partition_id: None,
        path: Some(vec![PathElement {
            id: None,
            kind: Some("a".to_string()),
            name: Some("test_value".to_string()),
        }]),
    };

    let identifier: IdentifierRepr<KindA, IdentifierNone> = key.try_into()?;

    assert_eq!("a", identifier.kind());
    assert!(
        Repr {
            y: "test_value".to_string()
        } == identifier.name.unwrap()
    );

    Ok(())
}
