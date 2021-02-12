use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use datastorers::transaction::TransactionConnection;
use datastorers::{
    delete_one, id, name, DatastoreClientError, DatastoreManaged, DatastoreParseError,
    DatastorersError, DatastorersQueryable, IdentifierId, IdentifierName, IdentifierNone, Kind,
    Operator, Order,
};

use crate::connection::create_test_connection;

use std::convert::TryInto;
mod connection;

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "Test"]
#[page_size = 2]
pub struct TestEntity {
    #[key]
    pub key: IdentifierId<Self>,

    #[version]
    pub version: Option<i64>,

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

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "Test"]
#[page_size = 2]
pub struct TestEntityOptional {
    #[key]
    pub key: IdentifierId<Self>,

    #[indexed]
    #[property = "Name"]
    pub prop_string: Option<String>,

    #[property = "bool_property"]
    pub prop_bool: Option<bool>,

    #[property = "int_property"]
    pub prop_int: Option<i64>,

    #[property = "str_array_property"]
    pub prop_string_array: Option<Vec<String>>,
}

#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "TestNameKey"]
pub struct TestEntityName {
    #[key]
    pub key: IdentifierName<Self>,

    pub prop_string: String,
}

impl Default for TestEntityOptional {
    fn default() -> Self {
        TestEntityOptional {
            key: IdentifierId::id(None, IdentifierNone::none()),
            prop_string: None,
            prop_bool: None,
            prop_int: None,
            prop_string_array: None,
        }
    }
}

fn generate_random_string(len: usize) -> String {
    thread_rng().sample_iter(&Alphanumeric).take(len).collect()
}

fn generate_random_bool() -> bool {
    let val = thread_rng().gen_range(0, 2);
    val != 0
}

fn generate_random_int() -> i64 {
    thread_rng().gen()
}

fn generate_random_id<T: Kind>() -> IdentifierId<T, IdentifierNone> {
    IdentifierId::id(Some(generate_random_int()), IdentifierNone::none())
}

fn generate_random_entity() -> TestEntity {
    TestEntity {
        key: id![None],
        version: None,
        prop_string: generate_random_string(10),
        prop_bool: generate_random_bool(),
        prop_int: generate_random_int(),
        prop_string_array: vec![],
    }
}

fn generate_entity_with_values(prop_string: String, prop_int: i64) -> TestEntity {
    TestEntity {
        key: id![None],
        version: None,
        prop_string,
        prop_bool: generate_random_bool(),
        prop_int,
        prop_string_array: vec![],
    }
}

fn assert_client_error<T>(
    result: Result<T, DatastorersError>,
    expected_error: DatastoreClientError,
) {
    match result {
        Ok(_) => panic!("expect no entity to be found"),
        Err(e) => match e {
            DatastorersError::DatastoreClientError(client_error) => assert_eq!(
                client_error, expected_error,
                "Expected error to be {}",
                expected_error
            ),
            _ => panic!("Expected DatastoreClientError"),
        },
    };
}

fn assert_parse_error<T>(result: Result<T, DatastorersError>, expected_error: DatastoreParseError) {
    match result {
        Ok(_) => panic!("expect no entity to be found"),
        Err(e) => match e {
            DatastorersError::ParseError(parse_error) => assert_eq!(
                parse_error, expected_error,
                "Expected error to be {}",
                expected_error
            ),
            _ => panic!("Expected DatastoreParseError"),
        },
    };
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_insert_and_update() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;
    let original_entity = generate_random_entity();
    let original_bool_value = original_entity.prop_bool;

    let mut test_entity = original_entity.commit(&connection).await?;
    assert!(test_entity.key.id.is_some());
    // Save id for later validations
    let id_after_insert = test_entity.id().id.unwrap();

    // Update same item
    test_entity.prop_bool = !original_bool_value;
    let updated = test_entity.commit(&connection).await?;

    // Shall have been updated
    assert_eq!(updated.prop_bool, !original_bool_value);

    // But id shall remain the same
    assert_eq!(id_after_insert, updated.id().id.unwrap());

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_get_by_id() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Insert an entity with some random values
    let entity = generate_random_entity();
    let original_string = entity.prop_string.clone();
    let original_int = entity.prop_int;

    let inserted = entity.commit(&connection).await?;

    // Try fetch with a random id, to validate that not found check works
    let random_id = generate_random_id::<TestEntity>();
    assert_client_error(
        TestEntity::get_one_by_id(&connection, &random_id).await,
        DatastoreClientError::NotFound,
    );

    // Success
    let inserted_id = &inserted.key;
    let fetched_entity = TestEntity::get_one_by_id(&connection, inserted_id).await?;

    // Validate content of the fetched entity
    assert_eq!(&original_string, &fetched_entity.prop_string);
    assert_eq!(original_int, fetched_entity.prop_int);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_get_by_property() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Save 3 entities, 2 with the same name
    let expected_result_entity = generate_random_entity().commit(&connection).await?;
    let duplicated_entity = generate_random_entity();
    duplicated_entity.clone().commit(&connection).await?;
    duplicated_entity.clone().commit(&connection).await?;

    assert_ne!(expected_result_entity.prop_int, duplicated_entity.prop_int);

    // Not found
    assert_client_error(
        TestEntity::get_one_by_prop_string(&connection, generate_random_string(10)).await,
        DatastoreClientError::NotFound,
    );

    // Multiple results
    assert_client_error(
        TestEntity::get_one_by_prop_string(&connection, duplicated_entity.prop_string).await,
        DatastoreClientError::AmbiguousResult,
    );

    // Success
    let fetched_entity =
        match TestEntity::get_one_by_prop_string(&connection, expected_result_entity.prop_string)
            .await
        {
            Ok(e) => e,
            Err(e) => panic!("Failed to fetch entity: {}", e),
        };
    assert_eq!(fetched_entity.prop_int, expected_result_entity.prop_int);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_get_collection_by_property() -> Result<(), DatastorersError> {
    let page_size = 2;
    let connection = create_test_connection().await;

    // Create some entities (5 of them, that is enough for this test since page size in test build is 2)
    let common_string_prop = generate_random_string(15);
    let mut int_props = vec![]; // Save all upserted int props so we can validate the result later
    let mut fetched_int_props = vec![];
    for _ in 0..5 {
        let mut entity = generate_random_entity();
        entity.prop_string = common_string_prop.clone();
        let inserted = entity.commit(&connection).await?;
        int_props.push(inserted.prop_int);
    }

    // Fetch first page
    let page = TestEntity::get_by_prop_string(&connection, common_string_prop).await?;

    // Validate it
    assert_eq!(page.result.len(), page_size);
    assert!(page.has_more_results);
    for val in page.result.iter() {
        fetched_int_props.push(val.prop_int);
    }

    // Fetch next page
    let page_two = page.get_next_page(&connection).await?;

    // Validate it
    assert_eq!(page_two.result.len(), page_size);
    assert!(page_two.has_more_results);
    for val in page_two.result.iter() {
        fetched_int_props.push(val.prop_int);
    }

    // Fetch last page
    let last_page = page_two.get_next_page(&connection).await?;

    assert_eq!(last_page.result.len(), 1); // Shall now only be one item!
    assert!(!last_page.has_more_results); // Shall now not have any more results!
    for val in last_page.result.iter() {
        fetched_int_props.push(val.prop_int);
    }

    // Try to fetch one more page (shall fail)
    assert_client_error(
        last_page.get_next_page(&connection).await,
        DatastoreClientError::NoMorePages,
    );

    // Compare the two int arrays to validate that all inserted items have been fetched
    fetched_int_props.sort_unstable(); // Sort in place
    int_props.sort_unstable(); // Sort in place
    assert_eq!(fetched_int_props, int_props);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_update_property() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Create and insert
    let original = generate_random_entity();
    let inserted = original.clone().commit(&connection).await?;

    // Get by prop, shall be same key as created
    let mut fetched =
        TestEntity::get_one_by_prop_string(&connection, original.prop_string.clone()).await?;
    assert_eq!(&inserted.key, &fetched.key);

    // Change the prop value and commit
    let new_string_prop = generate_random_string(10);
    fetched.prop_string = new_string_prop.clone();
    assert_ne!(&fetched.prop_string, &original.prop_string);
    fetched.commit(&connection).await?;

    // Get by old prop value => not found
    assert_client_error(
        TestEntity::get_one_by_prop_string(&connection, original.prop_string.clone()).await,
        DatastoreClientError::NotFound,
    );

    // Get by new prop value => entity shall be founfd, with the original key
    let fetched = TestEntity::get_one_by_prop_string(&connection, new_string_prop.clone()).await?;
    assert_eq!(&inserted.key, &fetched.key);
    assert_eq!(&new_string_prop, &fetched.prop_string);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_get_by_array_property() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Generate some test entities
    let string_value_a = generate_random_string(10);
    let string_value_b = generate_random_string(10);
    let string_value_c = generate_random_string(10);

    let mut entity_a = generate_random_entity();
    entity_a.prop_string_array = vec![string_value_a.clone(), string_value_b.clone()];
    let mut entity_b = generate_random_entity();
    entity_b.prop_string_array = vec![string_value_b.clone(), string_value_c.clone()];

    // Insert
    let inserted_a = entity_a.commit(&connection).await?;
    let inserted_b = entity_b.commit(&connection).await?;

    // Fetch for string_value_a => shall return entity_a
    let fetched_entity =
        TestEntity::get_one_by_prop_string_array(&connection, string_value_a).await?;
    assert_eq!(&inserted_a.key, &fetched_entity.key);

    // Fetch for string_value_c => shall return entity_b
    let fetched_entity =
        TestEntity::get_one_by_prop_string_array(&connection, string_value_c).await?;
    assert_eq!(&inserted_b.key, &fetched_entity.key);

    // Fetch for string_value_b => shall return multiple entities => error
    assert_client_error(
        TestEntity::get_one_by_prop_string_array(&connection, string_value_b).await,
        DatastoreClientError::AmbiguousResult,
    );

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_update_array_property() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Generate some test entities
    let string_value_a = generate_random_string(10);
    let string_value_b = generate_random_string(10);
    let string_value_c = generate_random_string(10);

    let mut entity = generate_random_entity();
    entity.prop_string_array = vec![string_value_a.clone(), string_value_b.clone()];

    // Insert
    let mut inserted = entity.commit(&connection).await?;
    let inserted_key = inserted.key.clone();

    // Fetch for string_value_a => shall return entity
    let fetched_entity =
        TestEntity::get_one_by_prop_string_array(&connection, string_value_a.clone()).await?;
    assert_eq!(&inserted.key, &fetched_entity.key);

    // Fetch for string_value_b => shall return entity
    let fetched_entity =
        TestEntity::get_one_by_prop_string_array(&connection, string_value_b.clone()).await?;
    assert_eq!(&inserted.key, &fetched_entity.key);

    // Fetch for string_value_c => shall return error not found
    assert_client_error(
        TestEntity::get_one_by_prop_string_array(&connection, string_value_c.clone()).await,
        DatastoreClientError::NotFound,
    );

    // Change array and commit
    inserted.prop_string_array.remove(0); // Remove string_value_a
    inserted.prop_string_array.push(string_value_c.clone()); // Push string_value_c
    inserted.commit(&connection).await?;

    // Try fetch again, now a shall fail and b + c shall return the inserted entity

    // Fetch for string_value_a => shall return error not found
    assert_client_error(
        TestEntity::get_one_by_prop_string_array(&connection, string_value_a).await,
        DatastoreClientError::NotFound,
    );

    // Fetch for string_value_b => shall return entity
    let fetched_entity =
        TestEntity::get_one_by_prop_string_array(&connection, string_value_b).await?;
    assert_eq!(&inserted_key, &fetched_entity.key);

    // Fetch for string_value_c => shall return entity
    let fetched_entity =
        TestEntity::get_one_by_prop_string_array(&connection, string_value_c).await?;
    assert_eq!(&inserted_key, &fetched_entity.key);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_delete() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    let inserted_a = generate_random_entity().commit(&connection).await?;
    let inserted_b = generate_random_entity().commit(&connection).await?;

    // Both shall be fetchable
    let fetched =
        TestEntity::get_one_by_prop_string(&connection, inserted_a.prop_string.clone()).await?;
    assert_eq!(&inserted_a.key, &fetched.key);
    let fetched =
        TestEntity::get_one_by_prop_string(&connection, inserted_b.prop_string.clone()).await?;
    assert_eq!(&inserted_b.key, &fetched.key);

    // Delete one
    let prop_string_b = inserted_b.prop_string.clone();
    inserted_b.delete(&connection).await?;

    // Only entity_a shall be fetchable
    let fetched =
        TestEntity::get_one_by_prop_string(&connection, inserted_a.prop_string.clone()).await?;
    assert_eq!(&inserted_a.key, &fetched.key);

    assert_client_error(
        TestEntity::get_one_by_prop_string(&connection, prop_string_b).await,
        DatastoreClientError::NotFound,
    );

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_optional_values() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;
    let def = TestEntityOptional::default();
    let mut inserted_empty = def.commit(&connection).await?;
    let inserted_id = inserted_empty.clone().key;
    // Set string and bool value and commit
    let string_value = generate_random_string(10);
    inserted_empty.prop_string = Some(string_value.clone());
    inserted_empty.prop_bool = Some(true);
    inserted_empty.commit(&connection).await?;

    // Fetch and validate that the inserted properties are saved
    let mut fetched_entity = TestEntityOptional::get_one_by_id(&connection, &inserted_id).await?;
    assert_eq!(&fetched_entity.prop_string, &Some(string_value.clone()));
    assert_eq!(&fetched_entity.prop_bool, &Some(true));
    assert_eq!(&fetched_entity.prop_int, &None);
    assert_eq!(&fetched_entity.prop_string_array, &None);

    // Try fetch with the non optional type, shalll fail since not all values are set!
    assert_parse_error(
        TestEntity::get_one_by_id(&connection, &id![inserted_id.id.unwrap()]).await,
        DatastoreParseError::NoSuchValue,
    );
    // Set the rest of the values
    let int_value = generate_random_int();
    fetched_entity.prop_int = Some(int_value);
    fetched_entity.prop_string_array = Some(vec![]);
    fetched_entity.commit(&connection).await?;

    // Fetch and validate result
    let fetched_entity =
        TestEntityOptional::get_one_by_prop_string(&connection, string_value.clone()).await?;
    assert_eq!(&fetched_entity.prop_string, &Some(string_value.clone()));
    assert_eq!(&fetched_entity.prop_bool, &Some(true));
    assert_eq!(&fetched_entity.prop_int, &Some(int_value));
    assert_eq!(&fetched_entity.prop_string_array, &Some(vec![]));

    // Now shall it also be possible to fetch the entity type without optionals
    let fetched_non_optional = TestEntity::get_one_by_prop_int(&connection, int_value).await?;
    assert_eq!(&fetched_non_optional.prop_string, &string_value);
    assert_eq!(&fetched_non_optional.prop_bool, &true);
    assert_eq!(&fetched_non_optional.prop_int, &int_value);
    let empty_vec: Vec<String> = vec![];
    assert_eq!(&fetched_non_optional.prop_string_array, &empty_vec);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_coliding_update() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;
    // Insert one entity
    let inserted = generate_random_entity().commit(&connection).await?;
    let inserted_id = &inserted.key;

    // Go fetch it two times (and change both fetched entities)
    let mut a = TestEntity::get_one_by_id(&connection, inserted_id).await?;
    let prop_int_a = generate_random_int();
    a.prop_int = prop_int_a;

    let mut b = TestEntity::get_one_by_id(&connection, inserted_id).await?;
    b.prop_int = generate_random_int();

    // Save the first one => we expect success
    a.commit(&connection).await?;

    // Save the second one => we expect error (collision)
    assert_client_error(
        b.commit(&connection).await,
        DatastoreClientError::DataConflict,
    );

    // Fetch one last time, the changes in a shall have been saved
    let fetched = TestEntity::get_one_by_id(&connection, inserted_id).await?;
    assert_eq!(prop_int_a, fetched.prop_int);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_coliding_delete() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;
    // Insert one entity
    let inserted = generate_random_entity().commit(&connection).await?;
    let inserted_id = &inserted.key;

    // Go fetch it two times (and change both fetched entities)
    let mut a = TestEntity::get_one_by_id(&connection, inserted_id).await?;
    let prop_int_a = generate_random_int();
    a.prop_int = prop_int_a;

    let mut b = TestEntity::get_one_by_id(&connection, inserted_id).await?;
    b.prop_int = generate_random_int();

    // Save the forst one => we expect success
    a.commit(&connection).await?;

    // Delete the second one => we expect error (collision)
    assert_client_error(
        b.delete(&connection).await,
        DatastoreClientError::DataConflict,
    );

    // Fetch one last time, the changes in a shall have been saved
    let fetched = TestEntity::get_one_by_id(&connection, inserted_id).await?;
    assert_eq!(prop_int_a, fetched.prop_int);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_transaction_with_update() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Create two entities
    let inserted = generate_random_entity().commit(&connection).await?;
    let inserted_id_a = &inserted.key;
    let original_prop_int_a = inserted.prop_int;
    let inserted = generate_random_entity().commit(&connection).await?;
    let inserted_id_b = &inserted.key;
    let original_prop_int_b = inserted.prop_int;

    // Create a transaction, use the transaction to fetch and modify both entities
    let mut transaction = TransactionConnection::begin_transaction(&connection).await?;

    let mut a = TestEntity::get_one_by_id(&transaction, inserted_id_a).await?;
    let prop_int_a = generate_random_int();
    a.prop_int = prop_int_a;
    transaction.push_save(a)?;

    let mut b = TestEntity::get_one_by_id(&transaction, inserted_id_b).await?;
    let prop_int_b = generate_random_int();
    b.prop_int = prop_int_b;
    transaction.push_save(b)?;

    // Transaction not commited, a and b shall have their original values
    let fetched_a = TestEntity::get_one_by_id(&connection, inserted_id_a).await?;
    assert_eq!(original_prop_int_a, fetched_a.prop_int);
    let fetched_b = TestEntity::get_one_by_id(&connection, inserted_id_b).await?;
    assert_eq!(original_prop_int_b, fetched_b.prop_int);

    // Commit transaction
    transaction.commit().await?;

    // Fetch and validate that both items got updated
    let fetched_a = TestEntity::get_one_by_id(&connection, inserted_id_a).await?;
    assert_eq!(fetched_a.prop_int, prop_int_a);

    let fetched_b = TestEntity::get_one_by_id(&connection, inserted_id_b).await?;
    assert_eq!(fetched_b.prop_int, prop_int_b);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_name_key() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;
    let entity = TestEntityName {
        key: name!["test"],
        prop_string: "String".to_string(),
    };

    // Delete if it exists
    let _ = delete_one(&connection, entity.clone().try_into()?).await;

    // Insert it
    let _ = entity.clone().commit(&connection).await?;

    // Fetch it
    let fetched = TestEntityName::get_one_by_id(&connection, &name!["test"]).await?;

    assert_eq!(&entity.key, &fetched.key);
    assert_eq!(&entity.prop_string, &fetched.prop_string);

    // Delete it again
    let _ = delete_one(&connection, entity.clone().try_into()?).await?;

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_query_by_props() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Save 3 entities, 2 with the same name
    let expected_result_entity = generate_random_entity().commit(&connection).await?;
    let mut duplicated_entity = generate_random_entity();
    // For those with duplicated names we add different numeric props in order
    // to be able to test queries with multiple conditions
    duplicated_entity.prop_int = generate_random_int();
    let _duplicated_one = duplicated_entity.clone().commit(&connection).await?;
    duplicated_entity.prop_int = generate_random_int();
    let duplicated_two = duplicated_entity.clone().commit(&connection).await?;

    assert_ne!(expected_result_entity.prop_int, duplicated_entity.prop_int);

    let fetched_entity = TestEntity::query()
        .filter(
            String::from("Name"),
            Operator::Equal,
            expected_result_entity.prop_string,
        )?
        .fetch_one(&connection)
        .await?;
    assert_eq!(fetched_entity.prop_int, expected_result_entity.prop_int);

    // Query on multiple properties, shall return one of the two entities with duplicated name
    let fetched_entity_dup_two = TestEntity::query()
        .filter(
            String::from("Name"),
            Operator::Equal,
            duplicated_entity.prop_string.clone(),
        )?
        .filter(
            String::from("int_property"),
            Operator::Equal,
            duplicated_two.prop_int,
        )?
        .fetch_one(&connection)
        .await?;
    assert_eq!(fetched_entity_dup_two.prop_int, duplicated_two.prop_int);

    // Query on with unknown value on the int property => no items shall be found
    assert_client_error(
        TestEntity::query()
            .filter(
                String::from("Name"),
                Operator::Equal,
                duplicated_entity.prop_string.clone(),
            )?
            .filter(String::from("int_property"), Operator::Equal, 42)?
            .fetch_one(&connection)
            .await,
        DatastoreClientError::NotFound,
    );

    // Query on name only => shall generate error (multiple items found)
    assert_client_error(
        TestEntity::query()
            .filter(
                String::from("Name"),
                Operator::Equal,
                duplicated_entity.prop_string.clone(),
            )?
            .fetch_one(&connection)
            .await,
        DatastoreClientError::AmbiguousResult,
    );

    // Query on name only, fetch multiple => shall return two items
    let fetched_page = TestEntity::query()
        .filter(
            String::from("Name"),
            Operator::Equal,
            duplicated_entity.prop_string.clone(),
        )?
        .fetch(&connection)
        .await?;
    assert_eq!(fetched_page.result.len(), 2);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_query_by_props_not_equal() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Save 3 entities, 2 with the same name
    let mut duplicated_entity = generate_random_entity();
    // For those with duplicated names we add different numeric props in order
    // to be able to test queries with multiple conditions
    duplicated_entity.prop_int = generate_random_int();
    let duplicated_one = duplicated_entity.clone().commit(&connection).await?;
    duplicated_entity.prop_int += 1;
    let duplicated_two = duplicated_entity.clone().commit(&connection).await?;

    let mut fetched_entity = TestEntity::query()
        .filter(
            String::from("Name"),
            Operator::Equal,
            duplicated_entity.prop_string.clone(),
        )?
        .filter(
            String::from("int_property"),
            Operator::GreaterThan,
            duplicated_one.prop_int,
        )?
        .fetch_one(&connection)
        .await?;
    // prop_int > duplicated_one.prop_int => we shall get duplicated_two
    assert_eq!(fetched_entity.prop_int, duplicated_two.prop_int);

    fetched_entity = TestEntity::query()
        .filter(
            String::from("Name"),
            Operator::Equal,
            duplicated_entity.prop_string.clone(),
        )?
        .filter(
            String::from("int_property"),
            Operator::LessThan,
            duplicated_two.prop_int,
        )?
        .fetch_one(&connection)
        .await?;
    // prop_int < duplicated_two.prop_int => we shall get duplicated_one
    assert_eq!(fetched_entity.prop_int, duplicated_one.prop_int);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_query_by_id() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;

    // Insert an entity with some random values
    let entity = generate_random_entity();
    let original_string = entity.prop_string.clone();
    let original_int = entity.prop_int;

    let inserted = entity.commit(&connection).await?;

    // Try fetch with a random id, to validate that not found check works
    let random_id = generate_random_id::<TestEntity>();
    assert_client_error(
        TestEntity::query().by_id(&connection, &random_id).await,
        DatastoreClientError::NotFound,
    );

    // Success
    let inserted_id = &inserted.key;
    let fetched_entity = TestEntity::query().by_id(&connection, inserted_id).await?;

    // Validate content of the fetched entity
    assert_eq!(&original_string, &fetched_entity.prop_string);
    assert_eq!(original_int, fetched_entity.prop_int);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_query_with_limit() -> Result<(), DatastorersError> {
    let page_size: i32 = 3;
    let connection = create_test_connection().await;

    // Create some entities, let us go get 7 of them
    let common_string_prop = generate_random_string(15);
    let mut int_props = vec![]; // Save all upserted int props so we can validate the result later
    let mut fetched_int_props = vec![];
    for _ in 0..7 {
        let mut entity = generate_random_entity();
        entity.prop_string = common_string_prop.clone();
        let inserted = entity.commit(&connection).await?;
        int_props.push(inserted.prop_int);
    }

    // Fetch first page
    let mut page = TestEntity::query()
        .limit(page_size)
        .filter(String::from("Name"), Operator::Equal, common_string_prop)?
        .fetch(&connection)
        .await?;

    // Validate it
    assert_eq!(page.result.len() as i32, page_size);
    assert!(page.has_more_results);
    for val in page.result.iter() {
        fetched_int_props.push(val.prop_int);
    }

    // Fetch more data, there shall be two more pages
    page = page.get_next_page(&connection).await?;
    for val in page.result.iter() {
        fetched_int_props.push(val.prop_int);
    }
    // Shal be more data
    assert!(page.has_more_results);
    page = page.get_next_page(&connection).await?;
    for val in page.result.iter() {
        fetched_int_props.push(val.prop_int);
    }
    // Shall be last page
    assert!(!page.has_more_results);

    // Try to fetch one more page (shall fail)
    assert_client_error(
        page.get_next_page(&connection).await,
        DatastoreClientError::NoMorePages,
    );

    // Compare the two int arrays to validate that all inserted items have been fetched
    fetched_int_props.sort_unstable(); // Sort in place
    int_props.sort_unstable(); // Sort in place
    assert_eq!(fetched_int_props, int_props);

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "integration_tests"), ignore)]
async fn test_query_with_order() -> Result<(), DatastorersError> {
    let connection = create_test_connection().await;
    let result_size: i32 = 3;

    let base_string_prop = generate_random_string(10);
    let base_int_prop = generate_random_int();
    // Insert four entities that can be used for various tests of result ordering
    let prop_int_a = base_int_prop + 10;
    let mut prop_string_a = base_string_prop.clone();
    prop_string_a.push('A');
    generate_entity_with_values(prop_string_a, prop_int_a)
        .commit(&connection)
        .await?;
    let prop_int_b = base_int_prop - 10;
    let mut prop_string_b = base_string_prop.clone();
    prop_string_b.push('A');
    generate_entity_with_values(prop_string_b, prop_int_b)
        .commit(&connection)
        .await?;
    let prop_int_c = base_int_prop + 20;
    let mut prop_string_c = base_string_prop.clone();
    prop_string_c.push('C');
    generate_entity_with_values(prop_string_c, prop_int_c)
        .commit(&connection)
        .await?;

    // Fetch all ordered by prop_string descending and prop_int descending
    let mut max_string_prop = base_string_prop.clone();
    max_string_prop.push('D');

    let mut fetched_int_props: Vec<i64> = TestEntity::query()
        .limit(result_size)
        .filter(
            String::from("Name"),
            Operator::GreaterThanOrEqual,
            base_string_prop.clone(),
        )?
        .filter(
            String::from("Name"),
            Operator::LessThan,
            max_string_prop.clone(),
        )?
        .order_by(String::from("Name"), Order::Descending)
        .order_by(String::from("int_property"), Order::Descending)
        .fetch(&connection)
        .await?
        .result
        .into_iter()
        .map(|e| e.prop_int)
        .collect();
    let mut expected_int_props = vec![prop_int_c, prop_int_a, prop_int_b];
    assert_eq!(fetched_int_props, expected_int_props);

    // Fetch all ordered by prop_string ascending + prop_int descending
    fetched_int_props = TestEntity::query()
        .limit(result_size)
        .filter(
            String::from("Name"),
            Operator::GreaterThanOrEqual,
            base_string_prop.clone(),
        )?
        .filter(
            String::from("Name"),
            Operator::LessThan,
            max_string_prop.clone(),
        )?
        .order_by(String::from("Name"), Order::Ascending)
        .order_by(String::from("int_property"), Order::Descending)
        .fetch(&connection)
        .await?
        .result
        .into_iter()
        .map(|e| e.prop_int)
        .collect();
    expected_int_props = vec![prop_int_a, prop_int_b, prop_int_c];
    assert_eq!(fetched_int_props, expected_int_props);

    // Fetch all ordered by prop_string ascending + prop_int ascending
    fetched_int_props = TestEntity::query()
        .limit(result_size)
        .filter(
            String::from("Name"),
            Operator::GreaterThanOrEqual,
            base_string_prop.clone(),
        )?
        .filter(
            String::from("Name"),
            Operator::LessThan,
            max_string_prop.clone(),
        )?
        .order_by(String::from("Name"), Order::Ascending)
        .order_by(String::from("int_property"), Order::Ascending)
        .fetch(&connection)
        .await?
        .result
        .into_iter()
        .map(|e| e.prop_int)
        .collect();
    expected_int_props = vec![prop_int_b, prop_int_a, prop_int_c];
    assert_eq!(fetched_int_props, expected_int_props);

    Ok(())
}
