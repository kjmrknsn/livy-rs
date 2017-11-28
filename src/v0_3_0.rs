use std::collections::HashMap;

/// Apache Livy REST API client
pub struct Client {
    base_url: String,
}

impl Client {
    /// Constructs a new `Client`.
    ///
    /// # Examples
    /// ```
    /// use livy::v0_3_0::Client;
    ///
    /// let client = Client::new("http://example.com:8998/");
    /// ```
    pub fn new(base_url: &str) -> Client {
        Client {
            base_url: String::from(base_url),
        }
    }
}

/// Session which represents an interactive shell.
pub struct Session {
    id: i64,
    app_id: String,
    owner: String,
    proxy_user: String,
    kind: SessionKind,
    log: Vec<String>,
    state: SessionState,
    appInfo: HashMap<String, String>,
}

/// Session state
pub enum SessionState {
    NotStarted,
    Starting,
    Idle,
    Busy,
    ShuttingDown,
    Error,
    Dead,
    Success,
}

/// Session kind
pub enum SessionKind {
    Spark,
    Pyspark,
    Pyspark3,
    Sparkr,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_new() {
        let base_url = "http://example.com:8998/";

        let client = Client::new(base_url);

        assert_eq!(base_url, client.base_url);
    }
}
