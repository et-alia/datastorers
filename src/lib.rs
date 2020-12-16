pub mod connection;
mod entity;
pub mod error;
pub mod transaction;
pub use crate::connection::DatastoreConnection;
pub use crate::entity::{
    DatastoreEntity, DatastoreEntityCollection, DatastoreProperties, DatastoreValue,
    ResultCollection,
};
pub use crate::error::{DatastoreClientError, DatastoreParseError, DatastorersError};

pub use datastore_entity_derives::DatastoreManaged;

use google_datastore1::schemas::{
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    Filter, Key, KindExpression, LookupRequest, LookupResponse, Mutation, MutationResult,
    PathElement, PropertyFilter, PropertyFilterOp, PropertyReference, Query,
    QueryResultBatchMoreResults, RunQueryRequest, RunQueryResponse, Value, ReadOptions
};

use std::convert::TryFrom;
use std::convert::TryInto;

const DEFAULT_PAGE_SIZE: i32 = 50;

pub fn get_one_by_id(
    id: i64,
    kind: String,
    connection: &impl DatastoreConnection,
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
        read_options: Some(ReadOptions {
            transaction: connection.get_transaction_id(),
            read_consistency: None,
        })
    };
    let resp: LookupResponse = projects
        .lookup(req, connection.get_project_name())
        .execute()?;

    match resp.found {
        Some(mut found) => match found.len() {
            0 => Err(DatastoreClientError::NotFound)?,
            1 => {
                let res = found.remove(0);
                let result: DatastoreEntity = res.try_into()?;
                Ok(result)
            }
            _ => Err(DatastoreClientError::AmbiguousResult)?,
        },
        None => Err(DatastoreClientError::NotFound)?,
    }
}

fn get_datastore_value_for_value<K: Into<DatastoreValue>>(value: K) -> Value {
    let datastore_value: DatastoreValue = value.into();
    datastore_value.into()
}

fn build_query_from_property(
    property_name: String,
    property_value: impl Into<DatastoreValue>,
    kind: String,
    limit: i32,
) -> Query {
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
    query.limit = Some(limit);

    return query;
}

pub fn get_one_by_property(
    property_name: String,
    property_value: impl Into<DatastoreValue>,
    kind: String,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntity, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();
    let mut req = RunQueryRequest::default();
    req.query = Some(build_query_from_property(
        property_name,
        property_value,
        kind,
        1,
    ));
    req.read_options = Some(ReadOptions{
        transaction: connection.get_transaction_id(),
        read_consistency: None,
    });

    let resp: RunQueryResponse = projects
        .run_query(req, connection.get_project_name())
        .execute()?;

    match resp.batch {
        Some(batch) => {
            let more_results = batch
                .more_results
                .ok_or(DatastoreClientError::ApiDataError)?;
            if more_results != QueryResultBatchMoreResults::NoMoreResults {
                Err(DatastoreClientError::AmbiguousResult)?
            }
            if let Some(mut found) = batch.entity_results {
                match found.len() {
                    0 => Err(DatastoreClientError::NotFound)?,
                    1 => {
                        let res = found.remove(0);
                        let result: DatastoreEntity = res.try_into()?;
                        Ok(result)
                    }
                    _ => Err(DatastoreClientError::AmbiguousResult)?,
                }
            } else {
                Err(DatastoreClientError::NotFound)?
            }
        }
        None => Err(DatastoreClientError::NotFound)?,
    }
}

fn get_page(
    query: Query,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntityCollection, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();
    let mut req = RunQueryRequest::default();
    req.query = Some(query.clone());
    let resp: RunQueryResponse = projects
        .run_query(req, connection.get_project_name())
        .execute()?;

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
        None => Err(DatastoreClientError::NotFound)?,
    }
}

pub fn get_by_property(
    property_name: String,
    property_value: impl Into<DatastoreValue>,
    kind: String,
    limit: Option<i32>,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntityCollection, DatastorersError> {
    let query = build_query_from_property(
        property_name,
        property_value,
        kind,
        limit.unwrap_or(DEFAULT_PAGE_SIZE),
    );
    get_page(query, connection)
}

impl<T> ResultCollection<T>
where
    T: TryFrom<DatastoreEntity, Error = DatastoreParseError>,
{
    pub fn get_next_page(
        self,
        connection: &impl DatastoreConnection,
    ) -> Result<ResultCollection<T>, DatastorersError> {
        if !self.has_more_results {
            Err(DatastoreClientError::NoMorePages)?
        }
        let mut query = self.query.ok_or(DatastoreClientError::ApiDataError)?;
        let end_cursor = self.end_cursor.ok_or(DatastoreClientError::ApiDataError)?;
        query.start_cursor = Some(end_cursor);

        let page: DatastoreEntityCollection = get_page(query, connection)?;
        let res: ResultCollection<T> = page.try_into()?;
        return Ok(res);
    }
}

fn commit(
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
    let begin_transaction: BeginTransactionResponse = builder.execute()?;

    let commit_request = projects.commit(
        CommitRequest {
            mode: None,
            mutations: Some(mutations),
            transaction: begin_transaction.transaction,
        },
        connection.get_project_name(),
    );

    commit_request.execute()
}

fn key_has_id(key: &Option<Key>) -> Result<bool, DatastoreClientError> {
    match key {
        Some(k) => {
            if let Some(path) = &k.path {
                if path.len() > 0 {
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
            Err(DatastoreClientError::DataConflict)?
        }
    }
    Ok(result.key.clone())
}

pub fn commit_one(
    entity: DatastoreEntity,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntity, DatastorersError> {
    let is_insert = !key_has_id(&entity.key())?;
    let base_version = entity.version();
    let mut result_entity = entity.clone();
    let ent: Entity = entity.try_into()?;

    let mut mutation = Mutation::default();
    mutation.upsert = Some(ent);
    mutation.base_version = base_version;
    let cre: CommitResponse = commit(vec![mutation], connection)?;

    // The commit result shall contain a key that we can assign to the entity in order to later
    // be able to update it
    if let Some(results) = &cre.mutation_results {
        match results.len() {
            0 => Err(DatastoreClientError::KeyAssignmentFailed)?,
            1 => {
                let assigned_key = parse_mutation_result(&results[0])?;
                if is_insert {
                    if let Some(key) = assigned_key {
                        result_entity.set_key(Some(key));
                    } else {
                        Err(DatastoreClientError::KeyAssignmentFailed)?
                    }
                }
            }
            _ => Err(DatastoreClientError::AmbiguousResult)?,
        }
    } else {
        Err(DatastoreClientError::KeyAssignmentFailed)?
    }
    Ok(result_entity)
}

pub fn delete_one(
    entity: DatastoreEntity,
    connection: &impl DatastoreConnection,
) -> Result<(), DatastorersError> {
    let key = entity.key().ok_or(DatastoreClientError::NotFound)?; // No key to delete

    let mut mutation = Mutation::default();
    mutation.delete = Some(key);
    mutation.base_version = entity.version();
    let cre: CommitResponse = commit(vec![mutation], connection)?;

    // Assert that we have a commit result
    if let Some(results) = &cre.mutation_results {
        match results.len() {
            0 => Err(DatastoreClientError::DeleteFailed)?,
            1 => parse_mutation_result(&results[0]).map(|_| ()), // Success
            _ => Err(DatastoreClientError::AmbiguousResult)?,
        }
    } else {
        Err(DatastoreClientError::DeleteFailed)?
    }
}
