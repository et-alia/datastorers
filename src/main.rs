extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;
extern crate google_datastore1 as datastore1;

extern crate datastore_entity;
use datastore_entity::DatastoreEntity;


use datastore1::LookupRequest;
use datastore1::Key;
use datastore1::PathElement;

use std::default::Default;
use datastore1::Datastore;
use yup_oauth2 as oauth;

// TODO - move from here to an integation test + make em configurable
static TEST_KEY: &str = "./test-key.json";
static TEST_PROJECT_NAME: &str = "hugop-238317";

pub trait DatastoreEntity<T> {

    fn from_result_map(hm: &std::collections::HashMap<String, datastore1::Value>) -> T;

}

pub trait Datastorers<T> {

    fn from_lookup_response(response: &datastore1::LookupResponse) -> T;

    fn lookup(id: &String) -> Option<T>;

}


#[derive(DatastoreEntity)]
#[derive(Debug)]
#[derive(Default)]
struct Game { // TODO - this is is just an entity used for testing - change trait to beeing a lib and move this to some integration test
    Name: String, // <- TODO: The names in the struct maps one to one to the property names in datastore
    Value: i32,   // But the uppercase names generates warnings <- change how this works?
    Bool: bool,
}


impl Datastorers<Game> for Game { // TODO - Change so this fellow gets implemented for all structs implementinf the DatastoreEntity trait - that is possible?
    fn from_lookup_response(response: &datastore1::LookupResponse) -> Game {
        // TODO - many unwrap without error checking - go add it
        Game::from_result_map(response.found.as_ref().unwrap().get(0).unwrap().entity.as_ref().unwrap().properties.as_ref().unwrap())
    }

    fn lookup(id: &String) -> Option<Game> { // TODO - Create some trait/struct representing a connection, let it contain the hub and project name
        let client_secret = oauth::service_account_key_from_file(&String::from(TEST_KEY)).unwrap(); // TODO - handle error - not just unwrap
        let access = yup_oauth2::ServiceAccountAccess::new(
            client_secret,
            hyper::Client::with_connector(hyper::net::HttpsConnector::new(hyper_rustls::TlsClient::new()))
        );
        let hub = Datastore::new(
            hyper::Client::with_connector(hyper::net::HttpsConnector::new(hyper_rustls::TlsClient::new())),
            access
        );

        let req = LookupRequest {
            keys: Some(vec!(Key {
                path: Some(vec!(PathElement {
                    kind: Some("Game".to_string()), // TODO - game is the name if the entity - add method on DatastoreEntity that can return the name?
                    id: Some(id.to_string()),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };

        let result = hub
            .projects()
            .lookup(req, TEST_PROJECT_NAME)
            .doit();

        match result {
            Err(_e) => { // TODO - do something with this error?
                None
            },
            Ok(res) => {
                let ret = Game::from_lookup_response(&res.1);
                Some(ret)
            }
        }
    
    }
}

fn main() {

    let game = Game::lookup(&String::from("5632499082330112")); // Testing!
    println!("Success: {:?}", game); // Validate result + move this to an integration test
    
}
