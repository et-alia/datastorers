# Datastorers

## Testing

### Integrtion tests

Integration test that reads and write to/from an actual gcp Datastore is implemented in `test/integration`.
The tests are conteolled via a feature flag, if flag not is set when running tests they will be ignored.

So, to run all local tests:
```
cargo run test
```

To also include integration tests:
```
cargo test --features integration_tests
```

In order for the integration tests to work, some configuration is required:

1. GCP Project configuration:
The project used for testing must have a Datastore Entity with the following properties:
*Name* - Type strig, indexed
*bool_property* - Type boolean, not indexed
*int_property* - Type integer, indexed

2. Environment setup:
The following environment variables must be set:
*GOOGLE_APPLICATION_CREDENTIALS* - Google application credentials
*TEST_PROJECT_NAME* - Name of GCP project used for testing.

*NOTE:* The tests adds random data to the datastore, but all data is not removed, without any cleanup actions the amount of data eventually grow large.