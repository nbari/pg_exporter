//! Integration tests for all collectors
//! These tests require a live `PostgreSQL` connection

#[allow(clippy::duplicate_mod)]
#[path = "../common/mod.rs"]
mod common;

pub mod activity;
pub mod database;
pub mod default;
pub mod index;
pub mod locks;
pub mod replication;
pub mod stat;
pub mod statements;
pub mod tls;
pub mod vacuum;
pub mod citus;
