//! # livy
//! Apache Livy REST API Client
//!
//! ## Supported Versions of Apache Livy
//! * 0.3.0
//! * 0.4.0

extern crate curl;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

/// Utilities for sending an HTTP request and receiving an HTTP response
pub mod http;
/// Apache Livy 0.3.0 REST API client
pub mod v0_3_0;
/// Apache Livy 0.4.0 REST API client
pub mod v0_4_0;
