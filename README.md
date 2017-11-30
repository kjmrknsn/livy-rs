# Apache Livy REST API Client
[![Crates.io](https://img.shields.io/crates/v/livy.svg)](https://crates.io/crates/livy)
[![codecov](https://codecov.io/gh/kjmrknsn/livy-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/kjmrknsn/livy-rs)
[![Build Status](https://travis-ci.org/kjmrknsn/livy-rs.svg?branch=master)](https://travis-ci.org/kjmrknsn/livy-rs)
[![Released API docs](https://docs.rs/livy/badge.svg)](http://docs.rs/livy)

## Abstract
This crate provides an [Apache Livy](https://livy.incubator.apache.org/) REST API client.

## Supported Versions of Apache Livy
* 0.3.0
* 0.4.0

## Current Status
This crate is still under development. There are some unimplemented APIs.

### Implemented APIs
* GET /sessions
* GET /sessions/{sessionId}
* GET /sessions/{sessionId}/state
* DELETE /sessions/{sessionId}
* GET /sessions/{sessionId}/logs
* GET /sessions/{sessionId}/statements
* GET /sessions/{sessionId}/statements/{statementId}
* POST /sessions/{sessionId}/statements/{statementId}/cancel

### Unimplemented APIs
* POST /sessions
* POST /sessions/{sessionId}/statements
* GET /batches
* POST /batches
* GET /batches/{batchId}
* GET /batches/{batchId}/state
* DELETE /batches/{batchId}
* GET /batches/{batchId}/log
