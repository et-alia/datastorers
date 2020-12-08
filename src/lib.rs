pub mod connection;
mod accesstoken;
mod entity;
pub use crate::entity::{DatastoreEntity, DatastoreProperties, DatastoreValue, DatastoreParseError};
pub use crate::connection::{DatastoreConnection, ConnectionError};

pub use datastore_entity_derives::DatastoreManaged;

use thiserror::Error;
use google_datastore1::schemas::{
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    Filter, Key, KindExpression, LookupRequest, LookupResponse, Mutation, PathElement,
    PropertyFilter, PropertyFilterOp, PropertyReference, Query, RunQueryRequest, RunQueryResponse,
    Value, QueryResultBatchMoreResults
};

use std::convert::TryInto;

#[derive(Error, Debug)]
pub enum DatastoreClientError {
    #[error("entity not found")]
    NotFound,
    #[error("multiple entities found, single result expected")]
    AmbigiousResult,
    #[error("failed to assign key to inserted entity")]
    KeyAssignmentFailed,
    #[error("Unexpected response data")]
    ApiDataError,
}

#[derive(Error, Debug)]
pub enum DatastorersError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),    
    #[error(transparent)]
    ParseError(#[from] DatastoreParseError),
    #[error(transparent)]
    DatastoreError(#[from] google_datastore1::Error),
    #[error(transparent)]
    DatastoreClientError(#[from] DatastoreClientError)
}


pub fn get_one_by_id(
    id: i64,
    kind: String,
    connection: &impl DatastoreConnection
) -> Result<DatastoreEntity, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();

    let key = Key {
        partition_id: None,
        path: Some(vec![PathElement {
            id: Some(id),
            kind: Some(kind),
            name: None,
        }]),
    };
    let req = LookupRequest {
        keys: Some(vec![key]),
        read_options: None,
    };
    let resp: LookupResponse = projects.lookup(req, connection.get_project_name())
        .execute()?;

    match resp.found {
        Some(mut found) => { 
            match found.len() {
                0 => Err(DatastoreClientError::NotFound)?,
                1 => {
                    if let Some(entity) = found.remove(0).entity {
                        let result: DatastoreEntity = entity.try_into()?;
                        Ok(result)
                    } else {
                        Err(DatastoreClientError::NotFound)?
                    }
                },
                _ => Err(DatastoreClientError::AmbigiousResult)?
            }
        }
        None => Err(DatastoreClientError::NotFound)?,
    }
}

fn get_datastore_value_for_value<K: Into<DatastoreValue>>(value: K) -> Value {
    let datastore_value: DatastoreValue = value.into();
    datastore_value.into()
}

pub fn get_one_by_property<K>(
    property_name: String,
    property_value: K,
    kind: String,
    connection: &impl DatastoreConnection
) -> Result<DatastoreEntity, DatastorersError>
where
    K: Into<DatastoreValue>
{
    let client = connection.get_client();
    let projects = client.projects();

    let mut req = RunQueryRequest::default();
    let mut filter = Filter::default();
    filter.property_filter = Some(PropertyFilter {
        property: Some(PropertyReference {
            name: Some(property_name),
        }),
        value: Some(get_datastore_value_for_value(property_value)),
        op: Some(PropertyFilterOp::Equal),
    });
    let mut query = Query::default();
    query.kind = Some(vec![KindExpression { name: Some(kind) }]);
    query.filter = Some(filter);
    query.limit = Some(1);
    req.query = Some(query);

    let resp: RunQueryResponse = projects
        .run_query(req, connection.get_project_name())
        .execute()?;

    match resp.batch {
        Some(batch) => { 
            let more_results = batch.more_results.ok_or(DatastoreClientError::ApiDataError)?;
            if more_results != QueryResultBatchMoreResults::NoMoreResults {
                Err(DatastoreClientError::AmbigiousResult)?
            }
            if let Some(mut found) = batch.entity_results {
                match found.len() {
                    0 => Err(DatastoreClientError::NotFound)?,
                    1 => {
                        if let Some(entity) = found.remove(0).entity {
                            let result: DatastoreEntity = entity.try_into()?;
                            Ok(result)
                        } else {
                            Err(DatastoreClientError::NotFound)?
                        }
                    },
                    _ => Err(DatastoreClientError::AmbigiousResult)?
                }
            } else {
                Err(DatastoreClientError::NotFound)?
            }
        },
        None => Err(DatastoreClientError::NotFound)?,
    }
}

fn generate_empty_key(kind: String) -> Key {
    Key {
        partition_id: None,
        path: Some(vec![PathElement {
            id: None,
            kind: Some(kind),
            name: None,
        }]),
    }
}

pub fn commit_one(
    entity: DatastoreEntity,
    kind: String,
    connection: &impl DatastoreConnection
) -> Result<DatastoreEntity, DatastorersError> {
    let mut result_entity = entity.clone();
    let client = connection.get_client();
    let projects = client.projects();
    let builder = projects.begin_transaction(
        BeginTransactionRequest {
            transaction_options: None,
        },
        connection.get_project_name(),
    );
    let begin_transaction: BeginTransactionResponse = builder.execute()?;
    let is_insert = !entity.has_key();
    let mut ent: Entity = entity.try_into()?;
    if !ent.key.is_some() {
        ent.key = Some(generate_empty_key(kind));
    }

    let mut mutation = Mutation::default();
    if is_insert {
        mutation.insert = Some(ent);
    } else {
        mutation.update = Some(ent);
    }
    let mutations: Vec<Mutation> = vec![mutation];

    let commit_request = projects.commit(
        CommitRequest {
            mode: None,
            mutations: Some(mutations),
            transaction: begin_transaction.transaction,
        },
        connection.get_project_name(),
    );

    let cre: CommitResponse = commit_request.execute()?;
            
    if is_insert {
        // The commit result shall contain a key that we can assign to the entity in order to later
        // be able to update it
        if let Some(results) = &cre.mutation_results {
            match results.len() {
                0 => Err(DatastoreClientError::KeyAssignmentFailed)?,
                1 => {
                    let mutation_result = &results[0];
                    result_entity.set_key(mutation_result.key.clone());
                },
                _ => Err(DatastoreClientError::AmbigiousResult)?,
            }
        } else {
            Err(DatastoreClientError::KeyAssignmentFailed)?
        }
    }

    Ok(result_entity)
}
