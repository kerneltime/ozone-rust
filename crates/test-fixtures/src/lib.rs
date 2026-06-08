//! Shared test fixtures: in-memory OM and SCM servers for driving the gateway
//! and datanode in integration tests without the real Java control plane.
//!
//! See: notetaker/Projects/Apache Ozone/S3 Gateway Rust/

#![forbid(unsafe_code)]

pub mod compliant_om;
pub mod compliant_scm;
pub mod fake_om;
