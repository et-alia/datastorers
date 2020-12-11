use datastore_entity::connection::DatastoreConnection;
use gcp_auth::{Error as GCPAuthError, Token};
use google_datastore1::Client;
use google_api_auth::GetAccessToken;

use std::error::Error;
use std::env;


//
// Google Access Token
//
#[derive(Debug)]
pub(crate) struct GoogleToken(pub Token);

impl GetAccessToken for GoogleToken {
    fn access_token(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(self.0.as_str().to_string())
    }
}

impl GoogleToken {
    #[tokio::main]
    pub(crate) async fn get_token() -> Result<Self, GCPAuthError> {
        let authentication_manager = gcp_auth::init().await?;
        let token = authentication_manager
            .get_token(&[
                "https://www.googleapis.com/auth/datastore",
            ])
            .await?;
        Ok(Self(token))
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
  pub fn from_project_name(project_name: String) -> Result<Connection, gcp_auth::Error> {
      let token = GoogleToken::get_token()?;
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

pub(crate) fn create_test_connection() -> Connection {
  let project_name = get_project_name();

  match Connection::from_project_name(project_name) {
      Ok(connection) => connection,
      Err(e) => panic!("Failed to setup google cloud connection: {}", e),
  }
}