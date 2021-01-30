#![allow(clippy::single_match)]

use std::convert::TryFrom;
use std::convert::TryInto;
use std::marker::PhantomData;

pub use crate::connection::DatastoreConnection;
pub use crate::entity::{
    DatastoreEntity, DatastoreEntityCollection, DatastoreProperties, DatastoreValue,
    ResultCollection,
};
pub use crate::error::*;
pub use crate::identifier::*;

pub use datastore_entity_derives::DatastoreManaged;

use crate::serialize::{Serialize, DatastoreSerializeError};

use google_datastore1::schemas::{
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    Filter, Key, KindExpression, LookupRequest, LookupResponse, Mutation, MutationResult,
    PropertyFilter, PropertyFilterOp, PropertyReference, Query, QueryResultBatchMoreResults,
    ReadOptions, RunQueryRequest, RunQueryResponse, CompositeFilterOp, CompositeFilter,
    PropertyOrder, PropertyOrderDirection
};

pub mod bytes;
pub mod connection;
pub mod deserialize;
mod entity;
pub mod error;
mod identifier;
pub mod serialize;
pub mod transaction;

const DEFAULT_PAGE_SIZE: i32 = 50;

pub trait Kind {
    /// Get the Entity's kind
    /// See [kind_str](Kind::kind_str) for a static trait method that returns the same value
    fn kind(&self) -> &'static str;

    /// Get the Entity's kind
    /// See [kind](Kind::kind) for an instance trait method that returns the same value
    fn kind_str() -> &'static str;
}

pub trait Pagable {
    // Get page size for kind, if one is present
    fn page_size() -> Option<i32>;
}

pub async fn get_one_by_id(
    connection: &impl DatastoreConnection,
    key_path: &impl KeyPath,
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
    connection: &impl DatastoreConnection,
    property_name: String,
    property_value: impl Serialize,
    kind: String,
) -> Result<DatastoreEntity, DatastorersError> {
    let property_filter = PropertyFilter {
        property: Some(PropertyReference {
            name: Some(property_name),
        }),
        value: property_value.serialize()?.map(|v| v.0),
        op: Some(PropertyFilterOp::Equal),
    };
    let filter = Filter {
        property_filter: Some(property_filter),
        ..Default::default()
    };
    query_one(Some(filter), kind, connection).await
}

pub async fn query_one(
    filter: Option<Filter>,
    kind: String,
    connection: &impl DatastoreConnection,
) -> Result<DatastoreEntity, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();

    let query = Query {
        kind: Some(vec![KindExpression { name: Some(kind) }]),
        filter: filter,
        limit: Some(1),
        ..Default::default()
    };
    let req = RunQueryRequest {
        query: Some(query),
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
    connection: &impl DatastoreConnection,
    query: Query,
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
    connection: &impl DatastoreConnection,
    property_name: String,
    property_value: impl Serialize,
    kind: String,
    limit: Option<i32>,
) -> Result<DatastoreEntityCollection, DatastorersError> {
    let query = build_query_from_property(
        property_name,
        property_value,
        kind,
        limit.unwrap_or(DEFAULT_PAGE_SIZE),
    )?;
    get_page(connection, query).await
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

        let page: DatastoreEntityCollection = get_page(connection, query).await?;
        let res: ResultCollection<T> = page.try_into()?;
        return Ok(res);
    }
}

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


pub trait DatastorersQueryable<E> {

    fn query() -> DatastorersQuery<E>;

    fn get_default_page_size() -> Option<i32>;
}

#[derive(Clone, Copy)]
pub enum Operator {
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl From<Operator> for PropertyFilterOp {
    fn from(item: Operator) -> Self {
        match item {
            Operator::Equal => PropertyFilterOp::Equal,
            Operator::GreaterThan => PropertyFilterOp::GreaterThan,
            Operator::GreaterThanOrEqual => PropertyFilterOp::GreaterThanOrEqual,
            Operator::LessThan => PropertyFilterOp::LessThan,
            Operator::LessThanOrEqual => PropertyFilterOp::LessThanOrEqual,
        }
    }
}

pub enum Order {
    Ascending,
    Descending,
}

impl From<Order> for PropertyOrderDirection {
    fn from(item: Order) -> Self {
        match item {
            Order::Ascending => PropertyOrderDirection::Ascending,
            Order::Descending => PropertyOrderDirection::Descending,
        }
    }
}

pub struct DatastorersPropertyFilterItem {
    pub value: DatastoreValue,
    pub operator: Operator,
    pub property: String,
}

impl From<DatastorersPropertyFilterItem> for Filter {
    fn from(filter_item: DatastorersPropertyFilterItem) -> Self {
        let operator = filter_item.operator.into();
        let filter = PropertyFilter {
            property: Some(PropertyReference {
                name: Some(String::from(filter_item.property)),
            }),
            value: Some(filter_item.value.into()),
            op: Some(operator),
        };
        Filter {
            property_filter: Some(filter),
            composite_filter: None,
        }
    }
}

pub struct DatastorersPropertyFilter {
    pub filter_items: Vec<DatastorersPropertyFilterItem>,
}

impl Default for DatastorersPropertyFilter {
    fn default() -> Self {
        DatastorersPropertyFilter {
            filter_items: Vec::new(),
        }
    }
}

impl DatastorersPropertyFilter {

    pub fn push(&mut self, filter_prop: String, operator: Operator, value: DatastoreValue) {
        self.filter_items.push(DatastorersPropertyFilterItem {
            value,
            operator,
            property: filter_prop
        });
    }
}

impl TryFrom<DatastorersPropertyFilter> for Filter {
    type Error = DatastorersError;

    fn try_from(mut val: DatastorersPropertyFilter) -> Result<Self, Self::Error> {
        match val.filter_items.len() {
            0 => Err(DatastoreClientError::NoFilterProps)?,
            1 => {
                let filter_item = val.filter_items.remove(0);
                Ok(filter_item.into())
            },
            _ => {
                let composite_filter = CompositeFilter {
                    op: Some(CompositeFilterOp::And), // Only allowed value
                    filters: Some(val.filter_items
                        .into_iter()
                        .map(|filter_item| filter_item.into())
                        .collect()
                    ),
                };
                Ok(Filter {
                    property_filter: None,
                    composite_filter: Some(composite_filter),
                })
            }
        }        
    }
}

pub struct DatastorersQuery<E> {

    entity: PhantomData<E>,
    filter: Option<DatastorersPropertyFilter>,
    limit: Option<i32>,
    order: Vec<PropertyOrder>,
}


impl<E> Default for DatastorersQuery<E> {

    fn default() -> Self {
        DatastorersQuery {
            entity: PhantomData,
            filter: None,
            limit: None,
            order: Vec::new(),
        }
    }
}

impl<E> DatastorersQuery<E>
where
    E: Kind + Pagable + DatastorersQueryable<E> + TryFrom<DatastoreEntity, Error = DatastorersError>
{

  pub fn filter(mut self, property_name: String, operator: Operator, value: impl Serialize) -> Result<DatastorersQuery<E>, DatastorersError>  {
    let ds_value = value.serialize()?.ok_or_else(|| DatastoreSerializeError::NoValueError)?;
    match self.filter {
        Some(ref mut filter) => {
            filter.push(property_name, operator, ds_value);
        },
        None => {
            let mut filter = DatastorersPropertyFilter::default();
            filter.push(property_name, operator, ds_value);
            self.filter = Some(filter);
        },
    };
    
    Ok(self)
  }

  pub fn limit(mut self, limit: i32) -> DatastorersQuery<E> {
    self.limit = Some(limit);

    self
  }

  pub fn order_by(mut self, property_name: String, order: Order) -> DatastorersQuery<E> {
    self.order.push(PropertyOrder {
        property: Some(PropertyReference {
            name: Some(property_name),
        }),
        direction: Some(order.into())
    });

    self
  }

  pub async fn by_id(self, connection: &impl DatastoreConnection, key_path: &impl KeyPath) -> Result<E, DatastorersError> {
    let query_result = get_one_by_id(
        connection,
        key_path,
    ).await?;
    let entity: E = query_result.try_into()?;
    Ok(entity)
  }


  pub async fn fetch_one(self, connection: &impl DatastoreConnection) -> Result<E, DatastorersError> {
    let filter = match self.filter {
        None => Ok(None),
        Some(prop_filter) => prop_filter.try_into().map(|f| Some(f)),
    }?;

    let query_result = query_one(
        filter,
        String::from(E::kind_str()),
        connection,
    ).await?;
    let entity: E = query_result.try_into()?;
    Ok(entity)
  }

  pub async fn fetch(self, connection: &impl DatastoreConnection) -> Result<ResultCollection<E>, DatastorersError> {
    let query = self.try_into()?;
    let page = get_page(connection, query).await?;
    let result = page.try_into()?;

    Ok(result)
  }
}

impl<E> TryFrom<DatastorersQuery<E>> for Query 
where
    E: Kind + DatastorersQueryable<E>
{
    type Error = DatastorersError;

    fn try_from(item: DatastorersQuery<E>) -> Result<Self, Self::Error> {
        let filter = match item.filter {
            None => Ok(None),
            Some(prop_filter) => prop_filter.try_into().map(|f| Some(f)),
        }?;
        let order = match item.order.len() {
            0 => None,
            _ => Some(item.order),
        };
        Ok(Query {
            kind: Some(vec![KindExpression { name: Some(String::from(E::kind_str())) }]),
            filter: filter,
            limit: item.limit.or_else(|| E::get_default_page_size().or_else(|| Some(DEFAULT_PAGE_SIZE))),
            order,
            ..Default::default()
        })
    }
}

impl <T> DatastorersQueryable<T> for T
    where T: Kind + Pagable + TryFrom<DatastoreEntity, Error = DatastorersError> 
{

    fn query() -> DatastorersQuery<T> {
        DatastorersQuery::default()
    }

    fn get_default_page_size() -> Option<i32> {
        T::page_size()
    }
}