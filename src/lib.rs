//! # livy
//! Apache Livy REST API Client
//!
//! ## Supported Versions of Apache Livy
//! * 0.3.0
//! * 0.4.0

extern crate reqwest;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

/// Utilities for sending an HTTP request and receiving an HTTP response
pub mod http;
/// Common utilities
pub mod utils;
/// Apache Livy 0.3.0 REST API client
pub mod v0_3_0;
