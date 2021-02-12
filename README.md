# Datastorers

Type safe Google Datastore access in Rust!

## Getting started

Currently Datastorers is only available as a git dependency.

Put this in your `Cargo.toml` if you are feeling adventurous:

```toml
[dependencies]
datastorers = { git = "https://github.com/et-alia/datastorers", branch = "master" }
```

If you are feeling less adventurous, use a specific commit:

```toml
[dependencies]
datastorers = { git = "https://github.com/et-alia/datastorers", rev = "<Pick your favorite SHA>" }
```

## Usage

These are the major parts of the datastorers API surface:

| Name                             | Type of API       | What it does                                                                                                 |
| -------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------ |
| `DatastoreManaged`               | Derive macro      | Generates code that enhances the functionality of a struct so that it can be used to model a datastore table |
| `DatastoreConnection`            | Trait             | Implement this trait to be able to connect to datastore. An example implementation exists in the integration tests. |
| `TransactionConnection`          | Struct            | A connection used for transactions. |
| `#[kind = "Kind"]`               | Attribute         | The kind of the datastore table |
| `#[page_size = 25]`              | Attribute         | How many items to fetch per page when using paged APIs |
| `#[key]`                         | Attribute         | Mark a property as the key. The key property must be of type `IdentifierId` or `IdentifierName` |
| `#[indexed]`                     | Attribute         | Mark a property as indexed, this is used in certain generated functions. |
| `#[property = "Name"]`           | Attribute         | By default property names refer to datastore table columns. Apply this attribute to use another name. |
| `IdentifierId<Kind, Ancestor>`   | Struct            | The id part of an identifier. The `Kind` parameter is `Self` in the simplest case, and `Ancestor` can be omitted unless there are ancestors in the key path. Can be further composed with `IdentifierName` for full key paths. |
| `id![<number>, path...]`         | Declarative macro | Helper macro used to create an id identifier. |
| `IdentifierName<Kind, Ancestor>` | Struct            | The name part of an identifier. Same rules as `IdentifierId`. |
| `name!["str", path...]`          | Declarative macro | Helper macro used to create a name identifier. |
| `IdentifierNone`                 | Struct            | The dangling part of an identifier, implicitly added to the end of any ancestor path. |

### Examples

More usage examples can be found in the [integration tests](tests/integration).

#### A basic usage example

```rust
#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "First"]
#[page_size = 2]
pub struct FirstEntity {
    #[key]
    pub key: IdentifierId<Self>,

    #[version]
    pub version: Option<i64>,

    #[indexed]
    #[property = "Name"]
    pub name: String,
}

async fn read_and_commit(connection: &impl DatastoreConnection) -> Result<(), DatastorersError> {
    let entity_instance = FirstEntity::get_one_by_name(
        connection,
        "test-name".to_string(),
    ).await?;
    entity_instance.commit(connection).await?;
    Ok(())
}

async fn create_first_and_commit(connection: &impl DatastoreConnection) -> Result<(), DatastorersError> {
    // A new entity is added to Google Datastore if the property marked as `#[key]`
    // has its first element set to `None`:
    let t = FirstEntity {
        key: id![None],
        version: None,
        name: "my string".to_string(),
    };
    t.commit(connection).await?;

    Ok(())
}
```

#### An example with a longer entity key path

```rust
#[derive(DatastoreManaged)]
#[kind = "Second"]
pub struct SecondEntity {
    #[key]
    pub key: IdentifierName<Self, IdentifierId<FirstEntity>>,

    pub data: String,
}

async fn create_second_and_commit(connection: &impl DatastoreConnection) -> Result<(), DatastorersError> {
    let t = SecondEntity {
        key: name![None, id![44444]],
        data: "My Data".to_string(),
    };

    t.commit(connection).await?;
    Ok(())
}
```

#### Transactions

Datastorers supports [datastore transactions](https://cloud.google.com/datastore/docs/concepts/transactions).
The transactions can be used to commit multiple update/create/delete modifications in one single request.

```rust
async fn use_a_transaction(connection: &impl DatastoreConnection) -> Result<(), DatastorersError> {
    let mut transaction = TransactionConnection::begin_transaction(connection).await?;

    // Add an entity that shall be saved when the transaction is committed
    let t = FirstEntity {
        key: id![None],
        version: None,
        name: "my string".to_string(),
    };

    // Add one or more operations to the transaction
    transaction.push_save(t);

    // Commit the transaction
    transaction.commit().await?;

    Ok(())
}
```

### Datastore connection

A connection to Google Datastore is created by implementing the `DatastoreConnection` trait.
See example implementation in the [integration tests](tests/integration/connection.rs).

### Read data

The struct deriving the `DatastoreManaged` macro will always contain the `get_one_by_id` method. It can be used to fetch one single entity based on its datastore id.

For each property that has the indexed attribute, getters will be generated based on the property name, for the example struct above will have:

```rust
impl TestEntity {
    async fn get_one_by_name(connection: &impl DatastoreConnection, value: String) -> Result<EntityName, DatastorersError> {
        // ...
    }

    async fn get_by_name(connection: &impl DatastoreConnection, value: String) -> Result<ResultCollection<EntityName>, DatastorersError> {
        // ...
    }
}
```

### Modify data

The struct deriving the `DatastoreManaged` macro will get methods for committing changes and to delete the entity:

```rust
impl TestEntity {
    async fn commit(connection: &impl DatastoreConnection) -> Result<EntityName, DatastorersError> {
        // ...
    }

    async fn delete(connection: &impl DatastoreConnection) -> Result<(), DatastorersError> {
        // ...
    }
}
```

## Testing

### Integration tests

Integration test that reads and write to/from an actual gcp Datastore is implemented in `test/integration`.
The tests are controlled via a feature flag, if flag not is set when running tests they will be ignored.

So, to run all local tests:

```shell
cargo run test
```

To also include integration tests:

```shell
cargo test --features integration_tests
```

In order for the integration tests to work, some configuration is required:

#### 1. GCP Project configuration:

The project used for testing must have a Datastore Entity with the kind `Test`,
that has the following properties:

* **Name** - Type string, indexed
* **bool_property** - Type boolean, not indexed
* **int_property** - Type integer, indexed

Some tests also require composit indexes in order to be able to run thier queries, to setup the
required indexes run:

```
gcloud datastore indexes create ./tests/integration/index.yaml  --project=$TEST_PROJECT_NAME --quiet 
```

For more details about indexing, see:
https://cloud.google.com/datastore/docs/concepts/indexes

#### 2. Environment setup:

The following environment variables must be set:

* **GOOGLE_APPLICATION_CREDENTIALS** - Google application credentials
* **TEST_PROJECT_NAME** - Name of GCP project used for testing.

**NOTE:** The tests adds random data to the datastore, but all data is not removed, without any cleanup actions the amount of data eventually grow large.

## Resources

### Datastore REST API documentation
https://cloud.google.com/datastore/docs/reference/data/rest

### google_datastore1

https://docs.rs/google-datastore1/1.0.14+20200524/google_datastore1/index.html

https://github.com/bes/google-datastore1-generated