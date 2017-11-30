use http;
use std::collections::HashMap;

/// Apache Livy REST API client
pub struct Client {
    url: String,
    client: http::Client,
}

impl Client {
    /// Constructs a new `Client`.
    ///
    /// # Examples
    /// ```
    /// use livy::v0_3_0::Client;
    ///
    /// let client = Client::new("http://example.com:8998");
    /// ```
    pub fn new(url: &str) -> Client {
        Client {
            url: http::remove_trailing_slash(url),
            client: http::Client::new(),
        }
    }

    /// Gets information of sessions and returns it.
    pub fn get_sessions(&self, from: Option<i64>, size: Option<i64>) -> Result<Sessions, String> {
        let params = http::params(vec![
            http::param("from", from),
            http::param("size", size)
        ]);

        self.client.get(format!("{}/sessions{}", self.url, params).as_str())
    }

    /// Gets information of a single session and returns it.
    pub fn get_session(&self, session_id: i64) -> Result<Session, String> {
        self.client.get(format!("{}/sessions/{}", self.url, session_id).as_str())
    }

    /// Gets session state information of a single session and returns it.
    pub fn get_session_state(&self, session_id: i64) -> Result<SessionStateOnly, String> {
        self.client.get(format!("{}/sessions/{}/state", self.url, session_id).as_str())
    }

    /// Deletes the session whose id is equal to `session_id`.
    pub fn delete_session(&self, session_id: i64) -> Result<(), String> {
        self.client.delete(format!("{}/sessions/{}", self.url, session_id).as_str())
    }

    /// Gets the log lines of a single session and returns them.
    pub fn get_session_log(&self, session_id: i64, from: Option<i64>, size: Option<i64>)-> Result<SessionLog, String> {
        let params = http::params(vec![
            http::param("from", from),
            http::param("size", size)
        ]);

        self.client.get(format!("{}/sessions/{}/log{}", self.url, session_id, params).as_str())
    }

    /// Gets the statements of a single session and returns them.
    pub fn get_statements(&self, session_id: i64) -> Result<Statements, String> {
        self.client.get(format!("{}/sessions/{}/statements", self.url, session_id).as_str())
    }

    /// Gets a single statement of a single session and returns it.
    pub fn get_statement(&self, session_id: i64, statement_id: i64) -> Result<Statement, String> {
        self.client.get(format!("{}/sessions/{}/statements/{}", self.url, session_id, statement_id).as_str())
    }

    /// Cancel a single statement.
    pub fn cancel_statement(&self, session_id: i64, statement_id: i64) -> Result<StatementCancelResult, String> {
        self.client.post(format!("{}/sessions/{}/statements/{}/cancel", self.url, session_id, statement_id).as_str(), String::new())
    }
}

/// Active interactive sessions
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sessions {
    from: Option<i64>,
    total: Option<i64>,
    sessions: Option<Vec<Session>>,
}

impl Sessions {
    /// Returns `from` of the sessions.
    pub fn from(&self) -> Option<i64> {
        self.from
    }

    /// Returns `total` of the sessions.
    pub fn total(&self) -> Option<i64> {
        self.total
    }

    /// Returns `sessions` of the sessions.
    pub fn sessions(&self) -> Option<&Vec<Session>> {
        self.sessions.as_ref()
    }
}

/// Session which represents an interactive shell
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    id: Option<i64>,
    app_id: Option<String>,
    owner: Option<String>,
    proxy_user: Option<String>,
    kind: Option<SessionKind>,
    log: Option<Vec<String>>,
    state: Option<SessionState>,
    app_info: Option<HashMap<String, String>>,
}

impl Session {
    /// Returns `id` of the session.
    pub fn id(&self) -> Option<i64> {
        self.id
    }

    /// Returns `app_id` of the session.
    pub fn app_id(&self) -> Option<&str> {
        self.app_id.as_ref().map(String::as_str)
    }

    /// Returns `owner` of the session.
    pub fn owner(&self) -> Option<&str> {
        self.owner.as_ref().map(String::as_str)
    }

    /// Returns `proxy_user` of the session.
    pub fn proxy_user(&self) -> Option<&str> {
        self.proxy_user.as_ref().map(String::as_str)
    }

    /// Returns `kind` of the session.
    pub fn kind(&self) -> Option<&SessionKind> {
        self.kind.as_ref()
    }

    /// Returns `log` of the session.
    pub fn log(&self) -> Option<&Vec<String>> {
        self.log.as_ref()
    }

    /// Returns `state` of the session.
    pub fn state(&self) -> Option<&SessionState> {
        self.state.as_ref()
    }

    /// Returns `app_info` of the session.
    pub fn app_info(&self) -> Option<&HashMap<String, String>> {
        self.app_info.as_ref()
    }
}

/// Session information which has only its state information
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SessionStateOnly {
    id: Option<i64>,
    state: Option<SessionState>,
}

impl SessionStateOnly {
    /// Returns `id` of the session.
    pub fn id(&self) -> Option<i64> {
        self.id
    }

    /// Returns `state` of the session.
    pub fn state(&self) -> Option<&SessionState> {
        self.state.as_ref()
    }
}

/// Session log
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SessionLog {
    id: Option<i64>,
    from: Option<i64>,
    total: Option<i64>,
    log: Option<Vec<String>>,
}

impl SessionLog {
    /// Returns `id` of the session.
    pub fn id(&self) -> Option<i64> {
        self.id
    }

    /// Returns `from` of the session log.
    pub fn from(&self) -> Option<i64> {
        self.from
    }

    /// Returns `total` of the session log.
    pub fn total(&self) -> Option<i64> {
        self.total
    }
    /// Returns `log` of the session log.
    pub fn log(&self) -> Option<&Vec<String>> {
        self.log.as_ref()
    }
}

/// Statements
#[derive(Debug, Deserialize, PartialEq)]
pub struct Statements {
    total_statements: Option<i64>,
    statements: Option<Vec<Statement>>,
}

impl Statements {
    /// Returns `total_statements` of the statements.
    pub fn total_statements(&self) -> Option<i64> {
        self.total_statements
    }

    /// Returns `statements` of the statements.
    pub fn statements(&self) -> Option<&Vec<Statement>> {
        self.statements.as_ref()
    }
}

/// Statement
#[derive(Debug, Deserialize, PartialEq)]
pub struct Statement {
    id: Option<i64>,
    state: Option<StatementState>,
    output: Option<StatementOutput>,
}

impl Statement {
    /// Returns `id` of the statement.
    pub fn id(&self) -> Option<i64> {
        self.id
    }

    /// Returns `state` of the statement.
    pub fn state(&self) -> Option<&StatementState> {
        self.state.as_ref()
    }

    /// Returns `output` of the statement.
    pub fn output(&self) -> Option<&StatementOutput> {
        self.output.as_ref()
    }
}

/// Statement output
#[derive(Debug, Deserialize, PartialEq)]
pub struct StatementOutput {
    status: Option<String>,
    execution_count: Option<i64>,
    data: Option<HashMap<String, String>>,
}

impl StatementOutput {
    /// Returns `status` of the statement output.
    pub fn status(&self) -> Option<&str> {
        self.status.as_ref().map(String::as_str)
    }

    /// Returns `execution_count` of the statement output.
    pub fn execution_count(&self) -> Option<i64> {
        self.execution_count
    }

    /// Returns `data` of the statement output.
    pub fn data(&self) -> Option<&HashMap<String, String>> {
        self.data.as_ref()
    }
}

/// Statement cancel result
#[derive(Debug, Deserialize, PartialEq)]
pub struct StatementCancelResult {
    msg: Option<String>,
}

impl StatementCancelResult {
    /// Returns `msg` of the statement cancel result.
    pub fn msg(&self) -> Option<&str> {
        self.msg.as_ref().map(String::as_str)
    }
}

/// Session state
#[derive(Debug, Deserialize, PartialEq)]
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
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SessionKind {
    Spark,
    Pyspark,
    Pyspark3,
    Sparkr,
}

/// Statement state
#[derive(Debug, Deserialize, PartialEq)]
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

    impl Sessions {
        fn some() -> Sessions {
            Sessions {
                from: Some(0),
                total: Some(1),
                sessions: Some(Vec::new()),
            }
        }

        fn none() -> Sessions {
            Sessions {
                from: None,
                total: None,
                sessions: None,
            }
        }
    }

    impl Session {
        fn some() -> Session {
            Session {
                id: Some(0),
                app_id: Some("app_id".to_string()),
                owner: Some("owner".to_string()),
                proxy_user: Some("proxy_user".to_string()),
                kind: Some(SessionKind::Spark),
                log: Some(Vec::new()),
                state: Some(SessionState::NotStarted),
                app_info: Some(HashMap::new()),
            }
        }

        fn none() -> Session {
            Session {
                id: None,
                app_id: None,
                owner: None,
                proxy_user: None,
                kind: None,
                log: None,
                state: None,
                app_info: None,
            }
        }
    }

    impl SessionStateOnly {
        fn some() -> SessionStateOnly {
            SessionStateOnly {
                id: Some(0),
                state: Some(SessionState::NotStarted),
            }
        }

        fn none() -> SessionStateOnly {
            SessionStateOnly {
                id: None,
                state: None,
            }
        }
    }

    impl SessionLog {
        fn some() -> SessionLog {
            SessionLog {
                id: Some(0),
                from: Some(1),
                total: Some(2),
                log: Some(Vec::new()),
            }
        }

        fn none() -> SessionLog {
            SessionLog {
                id: None,
                from: None,
                total: None,
                log: None,
            }
        }
    }

    impl Statements {
        fn some() -> Statements {
            Statements {
                total_statements: Some(0),
                statements: Some(Vec::new()),
            }
        }

        fn none() -> Statements {
            Statements {
                total_statements: None,
                statements: None,
            }
        }
    }

    impl Statement {
        fn some() -> Statement {
            Statement {
                id: Some(0),
                state: Some(StatementState::Waiting),
                output: Some(StatementOutput::some()),
            }
        }

        fn none() -> Statement {
            Statement {
                id: None,
                state: None,
                output: None,
            }
        }
    }

    impl StatementOutput {
        fn some() -> StatementOutput {
            StatementOutput {
                status: Some("status".to_string()),
                execution_count: Some(0),
                data: Some(HashMap::new()),
            }
        }

        fn none() -> StatementOutput {
            StatementOutput {
                status: None,
                execution_count: None,
                data: None,
            }
        }
    }

    impl StatementCancelResult {
        fn some() -> StatementCancelResult {
            StatementCancelResult {
                msg: Some(String::new()),
            }
        }

        fn none() -> StatementCancelResult {
            StatementCancelResult {
                msg: None,
            }
        }
    }

    #[test]
    fn test_client_new() {
        let url = "http://example.com:8998";
        let client = Client::new(url);
        assert_eq!(url, client.url);
    }

    #[test]
    fn test_sessions_from() {
        for sessions in vec![Sessions::some(), Sessions::none()] {
            assert_eq!(sessions.from, sessions.from());
        }
    }

    #[test]
    fn test_sessions_total() {
        for sessions in vec![Sessions::some(), Sessions::none()] {
            assert_eq!(sessions.total, sessions.total());
        }
    }

    #[test]
    fn test_sessions_sessions() {
        for sessions in vec![Sessions::some(), Sessions::none()] {
            assert_eq!(sessions.sessions.as_ref(), sessions.sessions());
        }
    }

    #[test]
    fn test_session_id() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.id, session.id());
        }
    }

    #[test]
    fn test_session_app_id() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.app_id.as_ref().map(String::as_str), session.app_id());
        }
    }

    #[test]
    fn test_session_owner() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.owner.as_ref().map(String::as_str), session.owner());
        }
    }

    #[test]
    fn test_session_proxy_user() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.proxy_user.as_ref().map(String::as_str), session.proxy_user());
        }
    }

    #[test]
    fn test_session_kind() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.kind.as_ref(), session.kind());
        }
    }

    #[test]
    fn test_session_log() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.log.as_ref(), session.log());
        }
    }

    #[test]
    fn test_session_state() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.state.as_ref(), session.state());
        }
    }

    #[test]
    fn test_session_app_info() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.app_info.as_ref(), session.app_info());
        }
    }

    #[test]
    fn test_session_state_only_id() {
        for session_state_only in vec![SessionStateOnly::some(), SessionStateOnly::none()] {
            assert_eq!(session_state_only.id, session_state_only.id());
        }
    }

    #[test]
    fn test_session_state_only_state() {
        for session_state_only in vec![SessionStateOnly::some(), SessionStateOnly::none()] {
            assert_eq!(session_state_only.state.as_ref(), session_state_only.state());
        }
    }

    #[test]
    fn test_session_log_id() {
        for session_log in vec![SessionLog::some(), SessionLog::none()] {
            assert_eq!(session_log.id, session_log.id());
        }
    }

    #[test]
    fn test_session_log_from() {
        for session_log in vec![SessionLog::some(), SessionLog::none()] {
            assert_eq!(session_log.from, session_log.from());
        }
    }

    #[test]
    fn test_session_log_total() {
        for session_log in vec![SessionLog::some(), SessionLog::none()] {
            assert_eq!(session_log.total, session_log.total());
        }
    }

    #[test]
    fn test_session_log_log() {
        for session_log in vec![SessionLog::some(), SessionLog::none()] {
            assert_eq!(session_log.log.as_ref(), session_log.log());
        }
    }

    #[test]
    fn test_statements_total_statements() {
        for statements in vec![Statements::some(), Statements::none()] {
            assert_eq!(statements.total_statements, statements.total_statements());
        }
    }

    #[test]
    fn test_statements_statements() {
        for statements in vec![Statements::some(), Statements::none()] {
            assert_eq!(statements.statements.as_ref(), statements.statements());
        }
    }

    #[test]
    fn test_statement_id() {
        for statement in vec![Statement::some(), Statement::none()] {
            assert_eq!(statement.id, statement.id());
        }
    }

    #[test]
    fn test_statement_state() {
        for statement in vec![Statement::some(), Statement::none()] {
            assert_eq!(statement.state.as_ref(), statement.state());
        }
    }

    #[test]
    fn test_statement_output() {
        for statement in vec![Statement::some(), Statement::none()] {
            assert_eq!(statement.output.as_ref(), statement.output());
        }
    }

    #[test]
    fn test_statement_output_status() {
        for statement_output in vec![StatementOutput::some(), StatementOutput::none()] {
            assert_eq!(statement_output.status.as_ref().map(String::as_str), statement_output.status());
        }
    }

    #[test]
    fn test_statement_output_execution_count() {
        for statement_output in vec![StatementOutput::some(), StatementOutput::none()] {
            assert_eq!(statement_output.execution_count, statement_output.execution_count());
        }
    }

    #[test]
    fn test_statement_output_data() {
        for statement_output in vec![StatementOutput::some(), StatementOutput::none()] {
            assert_eq!(statement_output.data.as_ref(), statement_output.data());
        }
    }

    #[test]
    fn test_statement_cancel_result_msg() {
        for statement_cancel_result in vec![StatementCancelResult::some(), StatementCancelResult::none()] {
            assert_eq!(statement_cancel_result.msg.as_ref().map(String::as_str), statement_cancel_result.msg());
        }
    }
}
