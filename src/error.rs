

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
    #[error("unexpected type in array item")]
    InvalidArrayValueFormat,
}


#[derive(Error, Debug, PartialEq)]
pub enum DatastoreClientError {
    #[error("entity not found")]
    NotFound,
    #[error("multiple entities found, single result expected")]
    AmbigiousResult,
    #[error("missing key, entity cannot be commited")]
    KeyMissing,
    #[error("failed to assign key to inserted entity")]
    KeyAssignmentFailed,
    #[error("delete operation failed")]
    DeleteFailed,
    #[error("unexpected response data")]
    ApiDataError,
    #[error("no more pages to fetch")]
    NoMorePages,
}

#[derive(Error, Debug)]
pub enum DatastorersError {
    #[error(transparent)]
    ParseError(#[from] DatastoreParseError),
    #[error(transparent)]
    DatastoreError(#[from] google_datastore1::Error),
    #[error(transparent)]
    DatastoreClientError(#[from] DatastoreClientError)
}
