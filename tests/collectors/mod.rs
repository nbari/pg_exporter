//! Integration tests for all collectors
//! These tests require a live PostgreSQL connection

#[path = "../common/mod.rs"]
mod common;

pub mod activity;
pub mod database;
pub mod default;
pub mod locks;
pub mod replication;
pub mod stat;
pub mod vacuum;
