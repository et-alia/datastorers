#![allow(clippy::single_match)]
#![warn(rust_2018_idioms)]
#![warn(unused)]
#![warn(rustdoc)]

pub use crate::connection::DatastoreConnection;
pub use crate::entity::{
    DatastoreEntity, DatastoreEntityCollection, DatastoreProperties, DatastoreValue, Kind, Pagable,
    ResultCollection,
};
pub use crate::error::*;
pub use crate::identifier::*;
pub use crate::query::*;
pub use crate::update::*;

pub use datastore_entity_derives::DatastoreManaged;

pub mod bytes;
pub mod connection;
pub mod deserialize;
mod entity;
pub mod error;
mod identifier;
pub mod query;
pub mod serialize;
pub mod transaction;
pub mod update;
