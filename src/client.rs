use http;
use http::Method;
use http::Method::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;

/// Apache Livy REST API client
pub struct Client {
    url: String,
    gssnegotiate: Option<bool>,
    username: Option<String>,
}

impl Client {
    /// Constructs a new `Client`.
    ///
    /// # Examples
    /// ```
    /// use livy::client::Client;
    ///
    /// let client = Client::new("http://example.com:8998", None, None);
    /// ```
    ///
    /// ```
    /// use livy::client::Client;
    ///
    /// let client = Client::new("http://example.com:8998", Some(true), Some("username".to_string()));
    /// ```
    pub fn new(url: &str, gssnegotiate: Option<bool>, username: Option<String>) -> Client {
        Client {
            url: http::remove_trailing_slash(url),
            gssnegotiate,
            username,
        }
    }

    /// Sends an HTTP request and returns the result.
    fn send<T: DeserializeOwned, U: Serialize>(&self, method: Method, path: &str, data: Option<U>) -> Result<T, String> {
        http::send(method,
                   format!("{}{}", self.url, path).as_str(),
                   data,
                   self.gssnegotiate.as_ref(),
                   self.username.as_ref().map(String::as_ref))
    }

    /// Sends an HTTP GET request and returns the result.
    fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        self.send(GET, path, None::<()>)
    }

    /// Sends an HTTP POST request and returns the result.
    fn post<T: DeserializeOwned, U: Serialize>(&self, path: &str, data: Option<U>) -> Result<T, String> {
        self.send(POST, path, data)
    }

    /// Sends an HTTP DELETE request and returns the result.
    fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        self.send(DELETE, path, None::<()>)
    }

    /// Gets information of sessions and returns it.
    ///
    /// # HTTP Request
    /// GET /sessions
    pub fn get_sessions(&self, from: Option<i64>, size: Option<i64>) -> Result<Sessions, String> {
        let params = http::params(vec![
            http::param("from", from),
            http::param("size", size)
        ]);

        self.get(format!("/sessions{}", params).as_str())
    }

    /// Creates a new session.
    ///
    /// # HTTP Request
    /// POST /sessions
    pub fn create_session(&self, new_session_request: NewSessionRequest) -> Result<Session, String> {
        self.post("/sessions", Some(new_session_request))
    }

    /// Gets information of a single session and returns it.
    ///
    /// # HTTP Request
    /// GET /sessions/{sessionId}
    pub fn get_session(&self, session_id: i64) -> Result<Session, String> {
        self.get(format!("/sessions/{}", session_id).as_str())
    }

    /// Gets session state information of a single session and returns it.
    ///
    /// # HTTP Request
    /// GET /sessions/{sessionId}/state
    pub fn get_session_state(&self, session_id: i64) -> Result<SessionStateOnly, String> {
        self.get(format!("/sessions/{}/state", session_id).as_str())
    }

    /// Kills the session whose id is equal to `session_id`.
    ///
    /// # HTTP Request
    /// DELETE /sessions/{sessionId}
    pub fn kill_session(&self, session_id: i64) -> Result<SessionKillResult, String> {
        self.delete(format!("/sessions/{}", session_id).as_str())
    }

    /// Gets the log lines of a single session and returns them.
    ///
    /// # HTTP Request
    /// GET /sessions/{sessionId}/log
    pub fn get_session_log(&self, session_id: i64, from: Option<i64>, size: Option<i64>)-> Result<SessionLog, String> {
        let params = http::params(vec![
            http::param("from", from),
            http::param("size", size)
        ]);

        self.get(format!("/sessions/{}/log{}", session_id, params).as_str())
    }

    /// Gets the statements of a single session and returns them.
    ///
    /// # HTTP Request
    /// GET /sessions/{sessionId}/statements
    pub fn get_statements(&self, session_id: i64) -> Result<Statements, String> {
        self.get(format!("/sessions/{}/statements", session_id).as_str())
    }

    /// Runs a statement in a session.
    ///
    /// # HTTP Request
    /// POST /sessions/{sessionId}/statements
    pub fn run_statement(&self, session_id: i64, run_statement_request: RunStatementRequest) -> Result<Statement, String> {
        self.post(format!("/sessions/{}/statements", session_id).as_str(), Some(run_statement_request))
    }

    /// Gets a single statement of a single session and returns it.
    ///
    /// # HTTP Request
    /// GET /sessions/{sessionId}/statements/{statementId}
    pub fn get_statement(&self, session_id: i64, statement_id: i64) -> Result<Statement, String> {
        self.get(format!("/sessions/{}/statements/{}", session_id, statement_id).as_str())
    }

    /// Cancel a single statement.
    ///
    /// # HTTP Request
    /// POST /sessions/{sessionId}/statements/{statementId}/cancel
    pub fn cancel_statement(&self, session_id: i64, statement_id: i64) -> Result<StatementCancelResult, String> {
        self.post(format!("/sessions/{}/statements/{}/cancel", session_id, statement_id).as_str(), None::<()>)
    }

    /// Gets information of batches and returns it.
    ///
    /// # HTTP Request
    /// GET /batches
    pub fn get_batches(&self, from: Option<i64>, size: Option<i64>) -> Result<Batches, String> {
        let params = http::params(vec![
            http::param("from", from),
            http::param("size", size)
        ]);

        self.get(format!("/batches{}", params).as_str())
    }

    /// Creates a new batch.
    ///
    /// # HTTP Request
    /// POST /batches
    pub fn create_batch(&self, new_batch_request: NewBatchRequest) -> Result<Batch, String> {
        self.post("/batches", Some(new_batch_request))
    }

    /// Gets a batch and returns it.
    ///
    /// # HTTP Request
    /// GET /batches/{batchId}
    pub fn get_batch(&self, batch_id: i64) -> Result<Batch, String> {
        self.get(format!("/batches/{}", batch_id).as_str())
    }

    /// Gets the state of batch session.
    ///
    /// # HTTP Request
    /// GET /batches/{batchId}/state
    pub fn get_batch_state(&self, batch_id: i64) -> Result<BatchStateOnly, String> {
        self.get(format!("/batches/{}/state", batch_id).as_str())
    }

    /// Kills the batch job.
    ///
    /// # HTTP Request
    /// DELETE /batches/{batchId}
    pub fn kill_batch(&self, batch_id: i64) -> Result<BatchKillResult, String> {
        self.delete(format!("/batches/{}", batch_id).as_str())
    }

    /// Gets the log lines from a batch and returns them.
    ///
    /// # HTTP Request
    /// GET /batches/{batchId}/log
    pub fn get_batch_log(&self, batch_id: i64, from: Option<i64>, size: Option<i64>) -> Result<BatchLog, String> {
        let params = http::params(vec![
            http::param("from", from),
            http::param("size", size)
        ]);

        self.get(format!("/batches/{}/log{}", batch_id, params).as_str())
    }
}

/// Active interactive sessions
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sessions {
    pub from: Option<i64>,
    pub total: Option<i64>,
    pub sessions: Option<Vec<Session>>,
}

/// New session request information
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewSessionRequest {
    pub kind: SessionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jars: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub py_files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_cores: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_cores: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_executors: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archives: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conf: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_timeout_in_second: Option<i64>,
}

/// Session which represents an interactive shell
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub id: Option<i64>,
    pub app_id: Option<String>,
    pub owner: Option<String>,
    pub proxy_user: Option<String>,
    pub kind: Option<SessionKind>,
    pub log: Option<Vec<String>>,
    pub state: Option<SessionState>,
    pub app_info: Option<HashMap<String, Option<String>>>,
}

/// Session information which has only its state information
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStateOnly {
    pub id: Option<i64>,
    pub state: Option<SessionState>,
}

/// Session kill result
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct SessionKillResult {
    pub msg: Option<String>,
}

/// Session log
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionLog {
    pub id: Option<i64>,
    pub from: Option<i64>,
    pub total: Option<i64>,
    pub log: Option<Vec<String>>,
}

/// Statements
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Statements {
    pub total_statements: Option<i64>,
    pub statements: Option<Vec<Statement>>,
}

/// Run statement request
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct RunStatementRequest {
    pub code: String,
}

/// Statement
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Statement {
    pub id: Option<i64>,
    pub state: Option<StatementState>,
    pub output: Option<StatementOutput>,
}

/// Statement output
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct StatementOutput {
    pub status: Option<String>,
    pub execution_count: Option<i64>,
    pub data: Option<HashMap<String, Option<String>>>,
}

/// Statement cancel result
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct StatementCancelResult {
    pub msg: Option<String>,
}

/// Batches information
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Batches {
    pub from: Option<i64>,
    pub total: Option<i64>,
    pub sessions: Option<Vec<Batch>>,
}

/// Single batch information
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub id: Option<i64>,
    pub app_id: Option<String>,
    pub app_info: Option<HashMap<String, Option<String>>>,
    pub log: Option<Vec<String>>,
    pub state: Option<String>,
}

/// New batch request information
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewBatchRequest {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jars: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub py_files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_cores: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_cores: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_executors: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archives: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conf: Option<HashMap<String, String>>,
}

/// Batch information which has only its state information.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchStateOnly {
    pub id: Option<i64>,
    pub state: Option<String>,
}

/// Batch kill result
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchKillResult {
    pub msg: Option<String>,
}

/// Batch log
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchLog {
    pub id: Option<i64>,
    pub from: Option<i64>,
    pub total: Option<i64>,
    pub log: Option<Vec<String>>,
}

/// Session state
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
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
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SessionKind {
    Spark,
    Pyspark,
    Pyspark3,
    Sparkr,
}

/// Statement state
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StatementState {
    Waiting,
    Running,
    Available,
    Error,
    Cancelling,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_new() {
        struct TestCase {
            url: &'static str,
            expected_url: String,
            gssnegotiate: Option<bool>,
            username: Option<String>,
        }

        let test_cases = vec![
            TestCase {
                url: "http://example.com:8998",
                expected_url: "http://example.com:8998".to_string(),
                gssnegotiate: None,
                username: None,
            },
            TestCase {
                url: "http://example.com:8998/",
                expected_url: "http://example.com:8998".to_string(),
                gssnegotiate: Some(false),
                username: Some("".to_string()),
            },
            TestCase {
                url: "http://example.com:8998",
                expected_url: "http://example.com:8998".to_string(),
                gssnegotiate: Some(true),
                username: Some("user".to_string()),
            },
        ];

        for test_case in test_cases {
            let client = Client::new(test_case.url, test_case.gssnegotiate.clone(), test_case.username.clone());

            assert_eq!(test_case.expected_url, client.url);
            assert_eq!(test_case.gssnegotiate, client.gssnegotiate);
            assert_eq!(test_case.username, client.username);
        }
    }
}
