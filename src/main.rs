use datastore_entity::{DatastoreEntity, DatastoreProperties, DatastoreManaged};
use datastore_entity::DatastoreFetch;

use google_datastore1::Client;
use google_datastore1::schemas::{Key, PathElement, LookupRequest, LookupResponse};

use gcp_auth::{Error as GCPAuthError, Token};
use google_api_auth::GetAccessToken;

use std::convert::TryInto;
use std::error::Error;

// TODO - move from here to an integation test + make em configurable
static TEST_KEY: &str = "./test-key.json";
static TEST_PROJECT_NAME: &str = "hugop-238317";


#[derive(DatastoreManaged, Debug)]
#[kind = "Game"]
struct Game {
    #[key]
    pub key: Option<Key>,

    pub Name: String, 
    pub Bool: bool,
}


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

fn test_get_entity() -> Result<Game, String> {
    let gtoken = GoogleToken::get_token()
        .map_err(|_e: gcp_auth::Error| -> String {"Failed to fetch entity".to_string()})?;
    //return Game::get_one_by_id(5632499082330112, gtoken, &TEST_PROJECT_NAME.to_string());
    return Game::get_one_by_Name("GGGG".to_string(), gtoken, &TEST_PROJECT_NAME.to_string());
}

fn main() {
    match test_get_entity() {
        Ok(game) => {
            println!("Success: {:#?}", game);
        },
        Err(err) => {
            println!("Error fetching data {:?}", err);
        }
    }    
}
