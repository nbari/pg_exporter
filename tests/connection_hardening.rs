#![allow(clippy::expect_used)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::panic)]
#![allow(clippy::unwrap_used)]

//! Standalone integration-test entry point for scrape connection hardening.
//!
//! These tests start an exporter against their own `PostgreSQL` 16 container. Keeping
//! them in a separate process prevents that exporter's process-wide version cache from
//! affecting collectors tested concurrently against the `PostgreSQL` version matrix.

mod common;

#[path = "collectors/connection_hardening.rs"]
mod connection_hardening;
