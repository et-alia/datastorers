#![allow(clippy::single_match)]

pub mod bytes;
pub mod connection;
pub mod deserialize;
mod entity;
pub mod error;
mod identifier;
pub mod serialize;
pub mod transaction;

pub use crate::connection::DatastoreConnection;
pub use crate::entity::{
    DatastoreEntity, DatastoreEntityCollection, DatastoreProperties, DatastoreValue,
    ResultCollection,
};
pub use crate::error::*;

pub use datastore_entity_derives::DatastoreManaged;

pub use identifier::*;

use google_datastore1::schemas::{
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    Filter, Key, KindExpression, LookupRequest, LookupResponse, Mutation, MutationResult,
    PropertyFilter, PropertyFilterOp, PropertyReference, Query, QueryResultBatchMoreResults,
    ReadOptions, RunQueryRequest, RunQueryResponse,
};

use crate::serialize::Serialize;
use std::convert::TryFrom;
use std::convert::TryInto;

const DEFAULT_PAGE_SIZE: i32 = 50;

pub trait Kind {
    /// Get the Entity's kind
    /// See [kind_str](Kind::kind_str) for a static trait method that returns the same value
    fn kind(&self) -> &'static str;

    /// Get the Entity's kind
    /// See [kind](Kind::kind) for an instance trait method that returns the same value
    fn kind_str() -> &'static str;
}

pub async fn get_one_by_id(
    key_path: &impl KeyPath,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntity, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();

    let key = key_path.get_key();
    let req = LookupRequest {
        keys: Some(vec![key]),
        read_options: Some(ReadOptions {
            transaction: connection.get_transaction_id(),
            read_consistency: None,
        }),
    };
    let resp: LookupResponse = projects
        .lookup(req, connection.get_project_name())
        .execute()
        .await?;

    match resp.found {
        Some(mut found) => match found.len() {
            0 => Err(DatastoreClientError::NotFound.into()),
            1 => {
                let res = found.remove(0);
                let result: DatastoreEntity = res.try_into()?;
                Ok(result)
            }
            _ => Err(DatastoreClientError::AmbiguousResult.into()),
        },
        None => Err(DatastoreClientError::NotFound.into()),
    }
}

fn build_query_from_property(
    property_name: String,
    property_value: impl Serialize,
    kind: String,
    limit: i32,
) -> Result<Query, DatastorersError> {
    let filter = Filter {
        property_filter: Some(PropertyFilter {
            property: Some(PropertyReference {
                name: Some(property_name),
            }),
            value: property_value.serialize()?.map(|v| v.0),
            op: Some(PropertyFilterOp::Equal),
        }),
        ..Default::default()
    };
    let query = Query {
        kind: Some(vec![KindExpression { name: Some(kind) }]),
        filter: Some(filter),
        limit: Some(limit),
        ..Default::default()
    };

    Ok(query)
}

pub async fn get_one_by_property(
    property_name: String,
    property_value: impl Serialize,
    kind: String,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntity, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();
    let req = RunQueryRequest {
        query: Some(build_query_from_property(
            property_name,
            property_value,
            kind,
            1,
        )?),
        read_options: Some(ReadOptions {
            transaction: connection.get_transaction_id(),
            read_consistency: None,
        }),
        ..Default::default()
    };

    let resp: RunQueryResponse = projects
        .run_query(req, connection.get_project_name())
        .execute()
        .await?;

    match resp.batch {
        Some(batch) => {
            let more_results = batch
                .more_results
                .ok_or(DatastoreClientError::ApiDataError)?;
            if more_results != QueryResultBatchMoreResults::NoMoreResults {
                return Err(DatastoreClientError::AmbiguousResult.into());
            }
            if let Some(mut found) = batch.entity_results {
                match found.len() {
                    0 => Err(DatastoreClientError::NotFound.into()),
                    1 => {
                        let res = found.remove(0);
                        let result: DatastoreEntity = res.try_into()?;
                        Ok(result)
                    }
                    _ => Err(DatastoreClientError::AmbiguousResult.into()),
                }
            } else {
                Err(DatastoreClientError::NotFound.into())
            }
        }
        None => Err(DatastoreClientError::NotFound.into()),
    }
}

async fn get_page(
    query: Query,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntityCollection, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();
    let req = RunQueryRequest {
        query: Some(query.clone()),
        ..Default::default()
    };
    let resp: RunQueryResponse = projects
        .run_query(req, connection.get_project_name())
        .execute()
        .await?;

    match resp.batch {
        Some(batch) => {
            let more_results = batch
                .more_results
                .ok_or(DatastoreClientError::ApiDataError)?;
            let has_more_results = more_results != QueryResultBatchMoreResults::NoMoreResults;
            let end_cursor = batch.end_cursor.ok_or(DatastoreClientError::ApiDataError)?;
            if let Some(found) = batch.entity_results {
                // Map results and return
                let mapped = found
                    .into_iter()
                    .map(|e| {
                        let result: DatastoreEntity = e.try_into()?;
                        Ok(result)
                    })
                    .collect::<Result<Vec<DatastoreEntity>, DatastorersError>>()?;
                Ok(DatastoreEntityCollection::from_result(
                    mapped,
                    query,
                    end_cursor,
                    has_more_results,
                ))
            } else {
                // Empty result
                Ok(DatastoreEntityCollection::default())
            }
        }
        None => Err(DatastoreClientError::NotFound.into()),
    }
}

pub async fn get_by_property(
    property_name: String,
    property_value: impl Serialize,
    kind: String,
    limit: Option<i32>,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntityCollection, DatastorersError> {
    let query = build_query_from_property(
        property_name,
        property_value,
        kind,
        limit.unwrap_or(DEFAULT_PAGE_SIZE),
    )?;
    get_page(query, connection).await
}

impl<T> ResultCollection<T>
where
    T: TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    pub async fn get_next_page(
        self,
        connection: &impl DatastoreConnection,
    ) -> Result<ResultCollection<T>, DatastorersError> {
        if !self.has_more_results {
            return Err(DatastoreClientError::NoMorePages.into());
        }
        let mut query = self.query.ok_or(DatastoreClientError::ApiDataError)?;
        let end_cursor = self.end_cursor.ok_or(DatastoreClientError::ApiDataError)?;
        query.start_cursor = Some(end_cursor);

        let page: DatastoreEntityCollection = get_page(query, connection).await?;
        let res: ResultCollection<T> = page.try_into()?;
        return Ok(res);
    }
}

async fn commit(
    mutations: Vec<Mutation>,
    connection: &impl DatastoreConnection,
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

fn key_has_id(key: &Option<Key>) -> Result<bool, DatastoreClientError> {
    match key {
        Some(k) => {
            if let Some(path) = &k.path {
                if !path.is_empty() {
                    return Ok(path[0].id.is_some());
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
    entity: DatastoreEntity,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntity, DatastorersError> {
    let is_insert = !key_has_id(&entity.key())?;
    let base_version = entity.version();
    let mut result_entity = entity.clone();
    let ent: Entity = entity.try_into()?;

    let mutation = Mutation {
        upsert: Some(ent),
        base_version,
        ..Default::default()
    };
    let cre: CommitResponse = commit(vec![mutation], connection).await?;

    // The commit result shall contain a key that we can assign to the entity in order to later
    // be able to update it
    if let Some(results) = &cre.mutation_results {
        match results.len() {
            0 => return Err(DatastoreClientError::KeyAssignmentFailed.into()),
            1 => {
                let assigned_key = parse_mutation_result(&results[0])?;
                if is_insert {
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
    entity: DatastoreEntity,
    connection: &impl DatastoreConnection,
) -> Result<(), DatastorersError> {
    let key = entity.key().ok_or(DatastoreClientError::NotFound)?; // No key to delete

    let mutation = Mutation {
        delete: Some(key),
        base_version: entity.version(),
        ..Default::default()
    };
    let cre: CommitResponse = commit(vec![mutation], connection).await?;

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
