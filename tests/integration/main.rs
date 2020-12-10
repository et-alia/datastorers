use datastore_entity::connection::Connection;
use datastore_entity::{DatastoreManaged, DatastoreClientError, DatastorersError};

use google_datastore1::schemas::Key;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

use std::convert::TryInto;
use std::env;

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "Test"]
pub struct TestEntity {
    #[key]
    pub key: Option<Key>,

    #[indexed]
    #[property = "Name"]
    pub prop_string: String,

    #[property = "bool_property"]
    pub prop_bool: bool,

    #[indexed]
    #[property = "int_property"]
    pub prop_int: i64,

    #[indexed]
    #[property = "str_array_property"]
    pub prop_string_array: Vec<String>,
}

fn get_project_name() -> String {
    let env_var_name = "TEST_PROJECT_NAME";
    match env::var(env_var_name) {
        Ok(val) => val,
        Err(e) => panic!("Failed to read project name from {}: {}", env_var_name, e),
    }
}

fn create_test_connection() -> Connection {
    let project_name = get_project_name();

    match Connection::from_project_name(project_name) {
        Ok(connection) => connection,
        Err(e) => panic!("Failed to setup google cloud connection: {}", e),
    }
}

fn generate_random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect()
}

fn generate_random_bool() -> bool {
    let val = thread_rng().gen_range(0, 2);
    val != 0
}

fn generate_random_int() -> i64 {
    thread_rng().gen()   
}

fn generate_random_entity() -> TestEntity {
    TestEntity {
        key: None,
        prop_string: generate_random_string(10),
        prop_bool: generate_random_bool(),
        prop_int: generate_random_int(),
        prop_string_array: vec![],
    }
}

fn insert_random_entity(connection: &Connection) -> TestEntity {
    insert_entity(generate_random_entity(), connection)
}

fn insert_entity(entity: TestEntity, connection: &Connection) -> TestEntity {
    match entity.commit(connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to insert entity: {}", e),
    }
}

#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_insert_and_update() {
    let connection = create_test_connection();
    let original_entity = generate_random_entity();
    let original_bool_value = original_entity.prop_bool;

    let mut test_entity = insert_entity(original_entity, &connection);
    assert!(test_entity.key.is_some());
    // Save id for later validations
    let id_after_insert = test_entity.id().unwrap().clone();

    // Update same item
    test_entity.prop_bool = !original_bool_value;
    let updated = match test_entity.commit(&connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to insert entity: {}", e),
    };

    // Shall have been updated
    assert_eq!(updated.prop_bool, !original_bool_value);

    // But id shall remain the same
    assert_eq!(&id_after_insert, updated.id().unwrap());
}

#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_get_by_id() {
    let connection = create_test_connection();

    // Insert an entity with some random values
    let entity = generate_random_entity();
    let original_string = entity.prop_string.clone();
    let original_int = entity.prop_int.clone();
    
    let inserted = insert_entity(entity, &connection);

    // Try fetch with a random id, to validate that not found check works
    let random_id = generate_random_int();
    match TestEntity::get_one_by_id(random_id, &connection) {
        Ok(_) => panic!("expect no entity to be found"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::NotFound => {} // Success!
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    }

    // Success
    let inserted_id = inserted.key.unwrap().path.unwrap()[0].id.unwrap();
    let fetched_entity = match TestEntity::get_one_by_id(inserted_id, &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };

    // Validate content of the fetched entity
    assert_eq!(&original_string, &fetched_entity.prop_string);
    assert_eq!(original_int, fetched_entity.prop_int);
}

#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_get_by_property() {
    let connection = create_test_connection();

    // Save 3 entities, 2 with the same name
    let expected_result_entity = insert_random_entity(&connection);
    let duplicated_entity = generate_random_entity();
    insert_entity(duplicated_entity.clone(), &connection);
    insert_entity(duplicated_entity.clone(), &connection);

    assert_ne!(expected_result_entity.prop_int, duplicated_entity.prop_int);

    // Not found
    match TestEntity::get_one_by_prop_string(generate_random_string(10), &connection) {
        Ok(_) => panic!("expect no entity to be found"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::NotFound => {} // Success!
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    }

    // Multiple results
    match TestEntity::get_one_by_prop_string(duplicated_entity.prop_string, &connection) {
        Ok(_) => panic!("expect a failure result"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::AmbigiousResult => {} // Success!
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    }

    // Success
    let fetched_entity = match TestEntity::get_one_by_prop_string(expected_result_entity.prop_string, &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(fetched_entity.prop_int, expected_result_entity.prop_int);
}

#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_update_property() {
    let connection = create_test_connection();

    // Create and insert
    let original = generate_random_entity();    
    let inserted = insert_entity(original.clone(), &connection);

    // Get by prop, shall be same key as created
    let mut fetched = match TestEntity::get_one_by_prop_string(original.prop_string.clone(), &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted.key, &fetched.key);

    // Change the prop value and commit
    let new_string_prop = generate_random_string(10);
    fetched.prop_string = new_string_prop.clone();
    assert_ne!(&fetched.prop_string, &original.prop_string);
    match fetched.commit(&connection) {
        Ok(_) => (),
        Err(e) => panic!("Failed to update entity: {}", e),
    };

    // Get by old prop value => not found
    match TestEntity::get_one_by_prop_string(original.prop_string.clone(), &connection) {
        Ok(_) => panic!("expect no entity to be found"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::NotFound => {} // Success!
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    }

    // Get by new prop value => entity shall be founfd, with the original key
    let fetched = match TestEntity::get_one_by_prop_string(new_string_prop.clone(), &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted.key, &fetched.key);
    assert_eq!(&new_string_prop, &fetched.prop_string);
}

#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_get_by_array_property() {
    let connection = create_test_connection();

    // Generate some test entities
    let string_value_a = generate_random_string(10);
    let string_value_b = generate_random_string(10);
    let string_value_c = generate_random_string(10);

    let mut entity_a = generate_random_entity();
    entity_a.prop_string_array = vec![string_value_a.clone(), string_value_b.clone()];
    let mut entity_b = generate_random_entity();
    entity_b.prop_string_array = vec![string_value_b.clone(), string_value_c.clone()];

    // Insert
    let inserted_a = insert_entity(entity_a, &connection);
    let inserted_b = insert_entity(entity_b, &connection);

    // Fetch for string_value_a => shall return entity_a
    let fetched_entity = match TestEntity::get_one_by_prop_string_array(string_value_a, &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted_a.key, &fetched_entity.key);


    // Fetch for string_value_c => shall return entity_b
    let fetched_entity = match TestEntity::get_one_by_prop_string_array(string_value_c, &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted_b.key, &fetched_entity.key);

    // Fetch for string_value_b => shall return multiple entities => error
    match TestEntity::get_one_by_prop_string_array(string_value_b, &connection) {
        Ok(_) => panic!("Expected error, but got success"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::AmbigiousResult => {}
                    _ => panic!("Expected ambigious result error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    };
}


#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_update_array_property() {
    let connection = create_test_connection();

    // Generate some test entities
    let string_value_a = generate_random_string(10);
    let string_value_b = generate_random_string(10);
    let string_value_c = generate_random_string(10);

    let mut entity = generate_random_entity();
    entity.prop_string_array = vec![string_value_a.clone(), string_value_b.clone()];
    
    // Insert
    let mut inserted = insert_entity(entity, &connection);
    let inserted_key = inserted.key.clone();

    // Fetch for string_value_a => shall return entity
    let fetched_entity = match TestEntity::get_one_by_prop_string_array(string_value_a.clone(), &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted.key, &fetched_entity.key);

    // Fetch for string_value_b => shall return entity
    let fetched_entity = match TestEntity::get_one_by_prop_string_array(string_value_b.clone(), &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted.key, &fetched_entity.key);

    // Fetch for string_value_c => shall return error not found
    match TestEntity::get_one_by_prop_string_array(string_value_c.clone(), &connection) {
        Ok(_) => panic!("Expected error, but got success"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::NotFound => {}
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    };

    // Change array and commit
    inserted.prop_string_array.remove(0); // Remove string_value_a
    inserted.prop_string_array.push(string_value_c.clone()); // Push string_value_c
    match inserted.commit(&connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to insert entity: {}", e),
    };

    // Try fetch again, now a shall fail and b + c shall return the inserted entity
    
    // Fetch for string_value_a => shall return error not found
    match TestEntity::get_one_by_prop_string_array(string_value_a, &connection) {
        Ok(_) => panic!("Expected error, but got success"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::NotFound => {}
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    };

    // Fetch for string_value_b => shall return entity
    let fetched_entity = match TestEntity::get_one_by_prop_string_array(string_value_b, &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted_key, &fetched_entity.key);

    // Fetch for string_value_c => shall return entity
    let fetched_entity = match TestEntity::get_one_by_prop_string_array(string_value_c, &connection) {
        Ok(e) => e,
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    assert_eq!(&inserted_key, &fetched_entity.key);
}


#[test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
fn test_delete() {
    let connection = create_test_connection();

    let inserted_a = insert_random_entity(&connection);
    let inserted_b = insert_random_entity(&connection);

    // Both shall be fetchable
    match TestEntity::get_one_by_prop_string(inserted_a.prop_string.clone(), &connection) {
        Ok(e) => assert_eq!(&inserted_a.key, &e.key),
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    
    match TestEntity::get_one_by_prop_string(inserted_b.prop_string.clone(), &connection) {
        Ok(e) => assert_eq!(&inserted_b.key, &e.key),
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };

    // Delete one
    let prop_string_b = inserted_b.prop_string.clone();
    match inserted_b.delete(&connection) {
        Ok(_) => {},
        Err(e) => panic!("Failed to fetch entity: {}", e),
    }

    // Only entity_a shall be fetchable
    match TestEntity::get_one_by_prop_string(inserted_a.prop_string.clone(), &connection) {
        Ok(e) => assert_eq!(&inserted_a.key, &e.key),
        Err(e) => panic!("Failed to fetch entity: {}", e),
    };
    
    match TestEntity::get_one_by_prop_string(prop_string_b, &connection) {
        Ok(_) => panic!("expect no entity to be found"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => {
                match client_error {
                    DatastoreClientError::NotFound => {} // Success!
                    _ => panic!("Expected not found error"),
                }
            },
            _ => panic!("Expected DatastoreClientError"),
        }
    };
}