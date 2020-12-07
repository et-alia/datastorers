use crate::accesstoken::GoogleToken;
use google_datastore1::Client;
use thiserror::Error;

pub trait DatastoreConnection {
    fn get_client(&self) -> &Client;
    fn get_project_name(&self) -> String;
}

pub struct Connection {
    client: Client,
    project_name: String,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    AuthError(#[from] gcp_auth::Error),
}

impl Connection {
    pub fn from_project_name(project_name: String) -> Result<Connection, ConnectionError> {
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
