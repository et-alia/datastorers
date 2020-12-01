use datastore_entity::{DatastoreEntity, DatastoreManaged};

use google_datastore1::schemas::Key;

use gcp_auth::{Error as GCPAuthError, Token};
use google_api_auth::GetAccessToken;

use std::convert::TryInto;
use std::error::Error;

// TODO - move from here to an integation test + make em configurable
static TEST_PROJECT_NAME: &str = "hugop-238317";


#[derive(DatastoreManaged, Clone, Debug)]
#[kind = "Game"]
struct Game {
    #[key]
    pub key: Option<Key>,

    #[indexed]
    #[property = "Name"]
    pub name: String, 

    #[property = "Bool"]
    pub bool_value: bool,
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

fn test_get() -> Result<Game, String> {
    // Test get by id
    let gtoken = GoogleToken::get_token()
        .map_err(|_e: gcp_auth::Error| -> String {"Failed to fetch entity".to_string()})?;
    let game = Game::get_one_by_id(5643280054222848, gtoken, &TEST_PROJECT_NAME.to_string())?;
    println!("FETCHED: {:#?}", game);

    // Test get by property
    let gtoken2 = GoogleToken::get_token()
        .map_err(|_e: gcp_auth::Error| -> String {"Failed to fetch entity".to_string()})?;
    let game = Game::get_one_by_name("GGGG".to_string(), gtoken2, &TEST_PROJECT_NAME.to_string())?;
    println!("FETCHED: {:#?}", game);
    Ok(game) 
}

fn test_insert() -> Result<Game, String> {
    let gtoken = GoogleToken::get_token()
        .map_err(|_e: gcp_auth::Error| -> String {"Failed to fetch entity".to_string()})?;
    // Insert
    let game = Game {
        key: None,
        name: "New! new".to_string(),
        bool_value: false,
    };
    println!("PRE INSERT: {:#?}", game);
    let mut game = game.commit(gtoken, &TEST_PROJECT_NAME.to_string())?;
    println!("POST INSERT: {:#?}", game);


    // Update same item
    let gtoken2 = GoogleToken::get_token()
        .map_err(|_e: gcp_auth::Error| -> String {"Failed to fetch entity".to_string()})?;
    game.bool_value = !game.bool_value;
    game.commit(gtoken2, &TEST_PROJECT_NAME.to_string())
}

fn main() {
    match test_insert() {
        Ok(game) => {
            println!("Success INSERT: {:#?}", game);
        },
        Err(err) => {
            println!("Error fetching data {:?}", err);
        }
    }

    match test_get() {
        Ok(game) => {
            println!("Success GET: {:#?}", game);
        },
        Err(err) => {
            println!("Error fetching data {:?}", err);
        }
    }
}
