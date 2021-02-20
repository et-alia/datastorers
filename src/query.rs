use std::convert::TryFrom;
use std::convert::TryInto;
use std::marker::PhantomData;

use crate::connection::DatastoreConnection;
use crate::entity::{
    DatastoreEntity, DatastoreEntityCollection, DatastoreValue, Kind, Pagable, ResultCollection,
};
use crate::error::{DatastoreClientError, DatastorersError};
use crate::identifier::KeyPath;

use crate::serialize::{DatastoreSerializeError, Serialize};

use google_datastore1::schemas::{
    CompositeFilter, CompositeFilterOp, Filter, KindExpression, LookupRequest, LookupResponse,
    PropertyFilter, PropertyFilterOp, PropertyOrder, PropertyOrderDirection, PropertyReference,
    Query, QueryResultBatchMoreResults, ReadOptions, RunQueryRequest, RunQueryResponse,
};

const DEFAULT_PAGE_SIZE: i32 = 50;

pub trait DatastorersQueryable<E>
where
    E: Kind
        + Pagable
        + DatastorersQueryable<E>
        + TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    fn query() -> DatastorersQuery<E>;

    fn get_default_page_size() -> Option<i32>;
}

impl<T> DatastorersQueryable<T> for T
where
    T: Kind + Pagable + TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    fn query() -> DatastorersQuery<T> {
        DatastorersQuery::default()
    }

    fn get_default_page_size() -> Option<i32> {
        T::page_size()
    }
}

pub struct DatastorersQuery<E>
where
    E: Kind
        + Pagable
        + DatastorersQueryable<E>
        + TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    entity: PhantomData<E>,
    filter: Option<DatastorersPropertyFilter>,
    limit: Option<i32>,
    order: Vec<PropertyOrder>,
}

impl<E> Default for DatastorersQuery<E>
where
    E: Kind
        + Pagable
        + DatastorersQueryable<E>
        + TryFrom<DatastoreEntity, Error = DatastorersError>,
{
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
    E: Kind
        + Pagable
        + DatastorersQueryable<E>
        + TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    pub fn filter(
        mut self,
        property_name: String,
        operator: Operator,
        value: impl Serialize,
    ) -> Result<DatastorersQuery<E>, DatastorersError> {
        let ds_value = value
            .serialize()?
            .ok_or(DatastoreSerializeError::NoValueError)?;
        match self.filter {
            Some(ref mut filter) => {
                filter.push(property_name, operator, ds_value);
            }
            None => {
                let mut filter = DatastorersPropertyFilter::default();
                filter.push(property_name, operator, ds_value);
                self.filter = Some(filter);
            }
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
            direction: Some(order.into()),
        });

        self
    }

    pub async fn by_id(
        self,
        connection: &impl DatastoreConnection,
        key_path: &impl KeyPath,
    ) -> Result<E, DatastorersError> {
        let query_result = get_one_by_id(connection, key_path).await?;
        let entity: E = query_result.try_into()?;
        Ok(entity)
    }

    pub async fn fetch_one(
        self,
        connection: &impl DatastoreConnection,
    ) -> Result<E, DatastorersError> {
        let filter = match self.filter {
            None => Ok(None),
            Some(prop_filter) => prop_filter.try_into().map(Some),
        }?;

        let query_result = query_one(connection, filter, String::from(E::kind_str())).await?;
        let entity: E = query_result.try_into()?;
        Ok(entity)
    }

    pub async fn fetch(
        self,
        connection: &impl DatastoreConnection,
    ) -> Result<ResultCollection<E>, DatastorersError> {
        let query = self.try_into()?;
        let page = get_page(connection, query).await?;
        let result = page.try_into()?;

        Ok(result)
    }
}

impl<E> TryFrom<DatastorersQuery<E>> for Query
where
    E: Kind
        + Pagable
        + DatastorersQueryable<E>
        + TryFrom<DatastoreEntity, Error = DatastorersError>,
{
    type Error = DatastorersError;

    fn try_from(item: DatastorersQuery<E>) -> Result<Self, Self::Error> {
        let filter = match item.filter {
            None => Ok(None),
            Some(prop_filter) => prop_filter.try_into().map(Some),
        }?;
        let order = match item.order.len() {
            0 => None,
            _ => Some(item.order),
        };
        Ok(Query {
            kind: Some(vec![KindExpression {
                name: Some(String::from(E::kind_str())),
            }]),
            filter,
            limit: item
                .limit
                .or_else(|| E::get_default_page_size().or(Some(DEFAULT_PAGE_SIZE))),
            order,
            ..Default::default()
        })
    }
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
                name: Some(filter_item.property),
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
            property: filter_prop,
        });
    }
}

impl TryFrom<DatastorersPropertyFilter> for Filter {
    type Error = DatastorersError;

    fn try_from(mut val: DatastorersPropertyFilter) -> Result<Self, Self::Error> {
        match val.filter_items.len() {
            0 => Err(DatastoreClientError::NoFilterProps.into()),
            1 => {
                let filter_item = val.filter_items.remove(0);
                Ok(filter_item.into())
            }
            _ => {
                let composite_filter = CompositeFilter {
                    op: Some(CompositeFilterOp::And), // Only allowed value
                    filters: Some(
                        val.filter_items
                            .into_iter()
                            .map(|filter_item| filter_item.into())
                            .collect(),
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

async fn get_one_by_id(
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

async fn query_one(
    connection: &impl DatastoreConnection,
    filter: Option<Filter>,
    kind: String,
) -> Result<DatastoreEntity, DatastorersError> {
    let client = connection.get_client();
    let projects = client.projects();

    let query = Query {
        kind: Some(vec![KindExpression { name: Some(kind) }]),
        filter,
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
