use crate::accesstoken::GoogleToken;
use google_datastore1::Client;


pub trait DatastoreConnection {
  fn get_client(&self) -> &Client;
  fn get_project_name(&self) -> String;
}

pub struct Connection {
  client: Client,
  project_name: String,
}

impl Connection {

  pub fn from_project_name(project_name: String) -> Result<Connection, String> {
      let token = GoogleToken::get_token()
          .map_err(|_e: gcp_auth::Error| -> String {"Failed to fetch entity".to_string()})?; // TODO - error!
      let client = Client::new(token);

      Ok(Connection{
          project_name,
          client
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