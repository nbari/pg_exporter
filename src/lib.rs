//! `PostgreSQL` metric exporter for Prometheus

pub mod cli;
pub mod collectors;
pub mod exporter;

#[cfg(test)]
#[path = "../tests/support/test_dsn.rs"]
mod test_dsn;

#[cfg(test)]
#[path = "../tests/support/trace_capture.rs"]
mod trace_capture;
