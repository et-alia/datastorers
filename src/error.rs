use crate::deserialize::DatastoreDeserializeError;
use crate::serialize::DatastoreSerializeError;
use thiserror::Error;

//
// DatastoreEntity related errors
//
#[derive(Error, Debug, PartialEq)]
pub enum DatastoreParseError {
    #[error("value not found")]
    NoSuchValue,
    #[error("no properties found on entity")]
    NoProperties,
    #[error("no data in result")]
    NoResult,
    #[error("unexpected type in array item")]
    InvalidArrayValueFormat,
}

#[derive(Error, Debug, PartialEq)]
pub enum DatastoreClientError {
    #[error("entity not found")]
    NotFound,
    #[error("multiple entities found, single result expected")]
    AmbiguousResult,
    #[error("missing key, entity cannot be commited")]
    KeyMissing,
    #[error("failed to assign key to inserted entity")]
    KeyAssignmentFailed,
    #[error("delete operation failed")]
    DeleteFailed,
    #[error("data conflict detected in commit")]
    DataConflict,
    #[error("unexpected response data")]
    ApiDataError,
    #[error("no more pages to fetch")]
    NoMorePages,
    #[error("cannot create transacion from transaction")]
    TransactionInProgress,
}

#[derive(Error, Debug, PartialEq)]
pub enum DatastoreKeyError {
    #[error("no key found")]
    NoKey,
    #[error("no key path found")]
    NoKeyPath,
    #[error("no key path element found")]
    NoKeyPathElement,
    #[error("no key kind found")]
    NoKind,
    #[error("key parsing failed, expected kind {expected:?} found {found:?}")]
    WrongKind {
        expected: &'static str,
        found: String,
    },
    #[error("expected identifier of type id (i64) but did not find it")]
    ExpectedId,
    #[error("expected identifier of type name (String) but did not find it")]
    ExpectedName,
}

#[derive(Error, Debug)]
pub enum DatastorersError {
    #[error(transparent)]
    ParseError(#[from] DatastoreParseError),
    #[error(transparent)]
    DatastoreError(#[from] google_datastore1::Error),
    #[error(transparent)]
    DatastoreClientError(#[from] DatastoreClientError),
    #[error(transparent)]
    DatastoreSerializeError(#[from] DatastoreSerializeError),
    #[error(transparent)]
    DatastoreDeserializeError(#[from] DatastoreDeserializeError),
    #[error(transparent)]
    DatastoreKeyError(#[from] DatastoreKeyError),
}
