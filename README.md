# Apache Livy REST API Client
[![Crates.io](https://img.shields.io/crates/v/livy.svg)](https://crates.io/crates/livy)
[![Build Status](https://travis-ci.org/kjmrknsn/livy-rs.svg?branch=master)](https://travis-ci.org/kjmrknsn/livy-rs)
[![Released API docs](https://docs.rs/livy/badge.svg)](http://docs.rs/livy)

## Abstract
This crate provides an [Apache Livy](https://livy.incubator.apache.org/) REST API client.

## Setup
First, add the following settings to `Cargo.toml`:

```toml
[dependencies]
livy = "0.2"
```

Next, add the following line to the crate root:

```rust
extern crate livy;
```

## Examples
```rust
extern crate livy;

use livy::client::Client;

fn main() {
    // http(s)://[livy_host]:[livy_port]
    let url = "http://example.com:8999";

    // The following configuration is useful to send an HTTP request
    // to a Livy REST API endpoint on a Kerberized Hadoop cluster.
    //
    // ```
    // let gssnegotiate = Some(true);
    // let username = Some("xxxx".to_string());
    // ```
    let gssnegotiate = None;
    let username = None;

    let client = Client::new(url, gssnegotiate, username);

    let from = None;
    let size = None;

    let res = match client.get_sessions(from, size) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            return;
        }
    };

    println!("response: {:#?}", res);

    /*
        response: Sessions {
            from: Some(
                0
            ),
            total: Some(
                1
            ),
            sessions: Some(
                [
                    Session {
                        id: Some(
                            1
                        ),
                        app_id: Some(
                            "application_1111111111111_11111"
                        ),
                        owner: Some (
                            "xxxx"
                        ),
                        proxy_user: Some(
                            "xxxx"
                        ),
                        kind: Some(
                            Spark
                        ),
                        log: Some(
                            [
                                "\t ApplicationMaster host: N/A",
                                "\t ApplicationMaster RPC port: -1",
                                "\t queue: default",
                                "\t start time: 1111111111111",
                                "\t final status: UNDEFINED",
                                "\t tracking URL: http://xxxx:8088/proxy/application_1111111111111_11111/",
                                "\t user: xxxx",
                                "17/12/01 15:27:38 INFO ShutdownHookManager: Shutdown hook called",
                                "17/12/01 15:27:38 INFO ShutdownHookManager: Deleting directory /tmp/spark-11111111-1111-1111-1111-111111111111",
                                "\nYARN Diagnostics: "
                            ]
                        ),
                        state: Some(
                            Starting
                        ),
                        app_info: Some(
                            {
                                "sparkUiUrl": Some(
                                    "http://xxxx:8088/proxy/application_1111111111111_11111/"
                                ),
                                "driverLogUrl": Some(
                                    "http://xxxx:8042/node/containerlogs/container_111_1111111111111_11111_11_111111/xxxx"
                                )
                            }
                        )
                    }
                ]
            )
        }
    */
}
```

## Documentation
[https://docs.rs/livy/](https://docs.rs/livy/)

## Supported Versions of Apache Livy
* 0.3.0
* 0.4.0
