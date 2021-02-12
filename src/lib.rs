#![allow(clippy::single_match)]

use std::convert::TryInto;

pub use crate::connection::DatastoreConnection;
pub use crate::entity::{
    DatastoreEntity, DatastoreEntityCollection, DatastoreProperties, DatastoreValue, Kind, Pagable,
    ResultCollection,
};
pub use crate::error::*;
pub use crate::identifier::*;
pub use crate::query::*;

pub use datastore_entity_derives::DatastoreManaged;

use google_datastore1::schemas::{
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity, Key,
    Mutation, MutationResult,
};

pub mod bytes;
pub mod connection;
pub mod deserialize;
mod entity;
pub mod error;
mod identifier;
pub mod query;
pub mod serialize;
pub mod transaction;

async fn commit(
    connection: &impl DatastoreConnection,
    mutations: Vec<Mutation>,
) -> Result<CommitResponse, google_datastore1::Error> {
    let client = connection.get_client();
    let projects = client.projects();
    let builder = projects.begin_transaction(
        BeginTransactionRequest {
            transaction_options: None,
        },
        connection.get_project_name(),
    );
    let begin_transaction: BeginTransactionResponse = builder.execute().await?;

    let commit_request = projects.commit(
        CommitRequest {
            mode: None,
            mutations: Some(mutations),
            transaction: begin_transaction.transaction,
        },
        connection.get_project_name(),
    );

    commit_request.execute().await
}

fn expects_key_after_commit(key: &Option<Key>) -> Result<bool, DatastoreClientError> {
    match key {
        Some(k) => {
            if let Some(path) = &k.path {
                if !path.is_empty() {
                    let first_path_element = &path[0];
                    return if first_path_element.name.is_some() || first_path_element.id.is_some() {
                        Ok(false)
                    } else {
                        Ok(true)
                    };
                }
            }
            Ok(false)
        }
        None => Err(DatastoreClientError::KeyMissing),
    }
}

fn parse_mutation_result(result: &MutationResult) -> Result<Option<Key>, DatastorersError> {
    if let Some(conflict_detected) = result.conflict_detected {
        if conflict_detected {
            return Err(DatastoreClientError::DataConflict.into());
        }
    }
    Ok(result.key.clone())
}

pub async fn commit_one(
    connection: &impl DatastoreConnection,
    entity: DatastoreEntity,
) -> Result<DatastoreEntity, DatastorersError> {
    let expects_key = expects_key_after_commit(&entity.key())?;
    let base_version = entity.version();
    let mut result_entity = entity.clone();
    let ent: Entity = entity.try_into()?;

    let mutation = Mutation {
        upsert: Some(ent),
        base_version,
        ..Default::default()
    };
    let cre: CommitResponse = commit(connection, vec![mutation]).await?;

    // The commit result shall contain a key that we can assign to the entity in order to later
    // be able to update it
    if let Some(results) = &cre.mutation_results {
        match results.len() {
            0 => return Err(DatastoreClientError::KeyAssignmentFailed.into()),
            1 => {
                // parse_mutation_result has a side effect - it checks if there are conflicts!
                // that's why it can't be moved into the if statement
                let assigned_key = parse_mutation_result(&results[0])?;
                if expects_key {
                    if let Some(key) = assigned_key {
                        result_entity.set_key(Some(key));
                    } else {
                        return Err(DatastoreClientError::KeyAssignmentFailed.into());
                    }
                }
            }
            _ => return Err(DatastoreClientError::AmbiguousResult.into()),
        }
    } else {
        return Err(DatastoreClientError::KeyAssignmentFailed.into());
    }
    Ok(result_entity)
}

pub async fn delete_one(
    connection: &impl DatastoreConnection,
    entity: DatastoreEntity,
) -> Result<(), DatastorersError> {
    let key = entity.key().ok_or(DatastoreClientError::NotFound)?; // No key to delete

    let mutation = Mutation {
        delete: Some(key),
        base_version: entity.version(),
        ..Default::default()
    };
    let cre: CommitResponse = commit(connection, vec![mutation]).await?;

    // Assert that we have a commit result
    if let Some(results) = &cre.mutation_results {
        match results.len() {
            0 => Err(DatastoreClientError::DeleteFailed.into()),
            1 => parse_mutation_result(&results[0]).map(|_| ()), // Success
            _ => Err(DatastoreClientError::AmbiguousResult.into()),
        }
    } else {
        Err(DatastoreClientError::DeleteFailed.into())
    }
}
