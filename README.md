# Datastorers

Type safe Google Datastore access in rust!

## Usage

### Basics

Derive `DatastoreManaged` on a struct:

```
#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "Test"]
#[page_size = 2]
pub struct TestEntity {
    #[key]
    pub key: Option<Key>,

    #[version]
    pub version: Option<i64>,

    #[indexed]
    #[property = "Name"]
    pub name: String,
}
```

Read an datastore entity:

```
let entity_instance = TestEntity::get_one_by_name(String::from("test-name", &connection))?;
```

Commit changes:

```
entity_instance.commit(&connection);
```

More usage examples can be found in the [integration tests](tests/integration).

### Datastore connection

A connection to Google Datastore is created by implementing the `DatastoreConnection` trait.
See example implementation in the [integration tests](tests/integration/connection.rs).

### Macro attributes and struct properties

* The struct must have a `kind` attribute, it will be used to map the struct to a kind of entity in Google Datastore.
* The struct must have a property with the `key` attribute, this property must be of type `Option<google_datastore1::schemas::Key>`, this property will contain the Google Datastore key once the item is fetched.
* The struct may have a property with the `version` attribute, if attribute is set the property must be of type `Option<i64>`. If the struct contains a version property it will be used when commiting changes to the entity to detect, and return error on, colliding updates of the entity.
* A property in the struct may have the `property` attribute. If set, the `property` attribute argument will be used as the Google Datastore property name.
* A property in the struct may have the `indexed` attribute. If set, getters will be generated for the property, if those shall work the corresponding property must be marked as indexed in Google Datastore.
* The struct may have the `page_size` attribute. If set the value will control the maximum page size when fetching multiple entities, if not set the default page size (50) will be used.


### Create data

A new entity is added to Google Datastore if a struct with `key` `None` is committed:

```
let = t TestEntity {
    key: None,
    version: None,
    name: String::from("my string),
}
t.commit(&connection)
```

### Read data

The struct deriving the `DatastoreManaged` macro will always contain the `get_one_by_id` method. It can be used to fetch one single entity based on its datastore id.

For each property that has the indexed attribute, getters will be generated based on the property name, for the example struct above will have:

```
TestEntity::get_one_by_name(value: String, connection: &impl DatastoreConnection) -> Result<TestEntity, DatastorersError>

TestEntity::get_by_name(value: String, connection: &impl DatastoreConnection) -> Result<ResultCollection<TestEntity>, DatastorersError>

```

### Modify data

The struct deriving the `DatastoreManaged` macro will get methods for commiting changes and to delete the entity:

```
TestEntity::commit(connection: &impl DatastoreConnection) -> Result<TestEntity, DatastorersError>

TestEntity::delete(connection: &impl DatastoreConnection) -> Result<(), DatastorersError>

```

### Transactions

Datastorers supports [datastore transactions](https://cloud.google.com/datastore/docs/concepts/transactions).
The transactions can be used to commit multiple update/create/delete modifications in one single request.

Crete a transaction:

```
let mut transaction = TransactionConnection::begin_transaction(&connection)?;
```

Add some entities that shall be saved when the transaction is committed:

```
let = t TestEntity {
    key: None,
    version: None,
    name: String::from("my string),
}
ttransaction.save(t)
```

Commit the transaction:

```
transaction.commit()?;
```

## Testing

### Integration tests

Integration test that reads and write to/from an actual gcp Datastore is implemented in `test/integration`.
The tests are controlled via a feature flag, if flag not is set when running tests they will be ignored.

So, to run all local tests:

```
cargo run test
```

To also include integration tests:

```
cargo test --features integration_tests
```

In order for the integration tests to work, some configuration is required:

#### 1. GCP Project configuration:

The project used for testing must have a Datastore Entity with the following properties:

* **Name** - Type string, indexed
* **bool_property** - Type boolean, not indexed
* **int_property** - Type integer, indexed

#### 2. Environment setup:

The following environment variables must be set:

* **GOOGLE_APPLICATION_CREDENTIALS** - Google application credentials
* **TEST_PROJECT_NAME** - Name of GCP project used for testing.

**NOTE:** The tests adds random data to the datastore, but all data is not removed, without any cleanup actions the amount of data eventually grow large.