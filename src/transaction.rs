use crate::connection::DatastoreConnection;
use crate::entity::DatastoreEntity;
use crate::error::{DatastoreClientError, DatastorersError};

use google_datastore1::schemas::{
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    Mutation,
};
use google_datastore1::Client;

use std::convert::TryInto;

pub struct TransactionConnection<'a> {
    connection: &'a dyn DatastoreConnection,
    transaction_id: String,
    mutations: Vec<Mutation>,
}

impl DatastoreConnection for TransactionConnection<'_> {
    fn get_client(&self) -> &Client {
        self.connection.get_client()
    }

    fn get_project_name(&self) -> String {
        self.connection.get_project_name()
    }

    fn get_transaction_id(&self) -> Option<String> {
        Some(self.transaction_id.clone())
    }
}

impl TransactionConnection<'_> {
    pub async fn begin_transaction(
        connection: &impl DatastoreConnection,
    ) -> Result<TransactionConnection<'_>, DatastorersError> {
        if connection.get_transaction_id().is_some() {
            // Transaction already in progress!
            return Err(DatastoreClientError::TransactionInProgress)?;
        }

        let client = connection.get_client();
        let projects = client.projects();
        let builder = projects.begin_transaction(
            BeginTransactionRequest {
                transaction_options: None,
            },
            connection.get_project_name(),
        );
        let begin_transaction: BeginTransactionResponse = builder.execute().await?;

        let transaction_id = begin_transaction
            .transaction
            .ok_or(DatastoreClientError::ApiDataError)?;
        Ok(TransactionConnection {
            connection,
            transaction_id,
            mutations: vec![],
        })
    }

    pub fn push_save(&mut self, item: impl Into<DatastoreEntity>) -> Result<(), DatastorersError> {
        let entity: DatastoreEntity = item.into();
        let base_version = entity.version();
        let ent: Entity = entity.try_into()?;

        let mut mutation = Mutation::default();
        mutation.upsert = Some(ent);
        mutation.base_version = base_version;

        self.mutations.push(mutation);

        Ok(())
    }

    pub fn push_delete(
        &mut self,
        item: impl Into<DatastoreEntity>,
    ) -> Result<(), DatastorersError> {
        let entity: DatastoreEntity = item.into();
        let base_version = entity.version();
        let mut mutation = Mutation::default();

        mutation.delete = entity.key();
        mutation.base_version = base_version;

        self.mutations.push(mutation);

        Ok(())
    }

    pub async fn commit(self) -> Result<(), DatastorersError> {
        let client = self.connection.get_client();
        let projects = client.projects();
        let commit_request = projects.commit(
            CommitRequest {
                mode: None,
                mutations: Some(self.mutations),
                transaction: Some(self.transaction_id),
            },
            self.connection.get_project_name(),
        );

        let cr: CommitResponse = commit_request.execute().await?;

        // Validate result for conflicts
        if let Some(results) = cr.mutation_results {
            for result in results {
                if let Some(conflict_detected) = result.conflict_detected {
                    if conflict_detected {
                        Err(DatastoreClientError::DataConflict)?
                    }
                }
            }
        } else {
            Err(DatastoreClientError::ApiDataError)?
        }
        Ok(())
    }
}
