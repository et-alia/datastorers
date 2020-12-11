use google_datastore1::Client;

pub trait DatastoreConnection {
    fn get_client(&self) -> &Client;
    fn get_project_name(&self) -> String;
}




