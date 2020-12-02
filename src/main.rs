use datastore_entity::{DatastoreEntity, DatastoreManaged};
use datastore_entity::connection::Connection;

use google_datastore1::schemas::{Key};

use std::convert::TryInto;

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

fn test_get(connection: &Connection) -> Result<Game, String> {
    // Test get by id
    let game = Game::get_one_by_id(5643280054222848, connection)?;
    println!("FETCHED: {:#?}", game);

    // Test get by property
    let game = Game::get_one_by_name("GGGG".to_string(), connection)?;
    println!("FETCHED: {:#?}", game);
    Ok(game) 
}

fn test_insert(connection: &Connection) -> Result<Game, String> {
    // Insert
    let game = Game {
        key: None,
        name: "New! new".to_string(),
        bool_value: false,
    };
    println!("PRE INSERT: {:#?}", game);
    let mut game = game.commit(connection)?;
    println!("POST INSERT: {:#?}", game);


    // Update same item
    game.bool_value = !game.bool_value;
    let updated = game.commit(connection)?;
    println!("POST UPDATE: {:#?}", updated);

    Ok(updated)
}


fn run_tests() -> Result<(), String> {
    let connection = Connection::from_project_name(TEST_PROJECT_NAME.to_string())?;
    match test_insert(&connection) {
        Ok(game) => {
            println!("SUCCESS test_insert: {:#?}", game);
        },
        Err(err) => {
            println!("ERROR running test_insert {:?}", err);
        }
    }

    match test_get(&connection) {
        Ok(game) => {
            println!("SUCCESS test_get: {:#?}", game);
        },
        Err(err) => {
            println!("ERROR running test_get {:?}", err);
        }
    }
    Ok(())
}

fn main() {
    match run_tests() {
        Ok(()) => {
            println!("Success");
        },
        Err(err) => {
            println!("Error fetching data {:?}", err);
        }
    }
}
