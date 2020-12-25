use async_trait::async_trait;
use datastore_entity::connection::DatastoreConnection;
use gcp_auth::{AuthenticationManager, Error as GCPAuthError};
use google_api_auth::GetAccessToken;
use google_datastore1::Client;

use std::env;
use std::error::Error;
use std::fmt::{Debug, Formatter};

//
// An opaque newtype type that can generate access tokens
//
pub(crate) struct GoogleAuthentication(AuthenticationManager);

impl Debug for GoogleAuthentication {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleToken with AuthenticationManager")
    }
}

#[async_trait]
impl GetAccessToken for GoogleAuthentication {
    async fn access_token(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let token = self
            .0
            .get_token(&[google_datastore1::scopes::DATASTORE])
            .await?;
        Ok(token.as_str().to_string())
    }
}

impl GoogleAuthentication {
    pub(crate) async fn get_token() -> Result<Self, GCPAuthError> {
        let authentication_manager = gcp_auth::init().await?;
        Ok(Self(authentication_manager))
    }
}

//
// Implement a DatastoreConnection to be used in integration tests
//
pub struct Connection {
    client: Client,
    project_name: String,
}

impl Connection {
    pub async fn from_project_name(project_name: String) -> Result<Connection, gcp_auth::Error> {
        let token = GoogleAuthentication::get_token().await?;
        let client = Client::new(token);

        Ok(Connection {
            project_name,
            client,
        })
    }
}

impl DatastoreConnection for Connection {
    fn get_client(&self) -> &Client {
        &self.client
    }

    fn get_project_name(&self) -> String {
        self.project_name.clone()
    }
}

//
// Utility to create test connection
//
fn get_project_name() -> String {
    let env_var_name = "TEST_PROJECT_NAME";
    match env::var(env_var_name) {
        Ok(val) => val,
        Err(e) => panic!("Failed to read project name from {}: {}", env_var_name, e),
    }
}

pub(crate) async fn create_test_connection() -> Connection {
    let project_name = get_project_name();

    match Connection::from_project_name(project_name).await {
        Ok(connection) => connection,
        Err(e) => panic!("Failed to setup google cloud connection: {}", e),
    }
}
