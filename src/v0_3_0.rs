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
            url: remove_trailing_slash(url),
            client: http::Client::new(),
        }
    }



    /// Gets information of a single session.
    pub fn get_session(&self, session_id: i64) -> Result<Session, String> {
        self.client.get(format!("{}/sessions/{}", self.url, session_id).as_str())
    }
}

/// Session which represents an interactive shell.
#[derive(Debug, Deserialize)]
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

/// Removes the trailing slash of `s` if it exists,
/// constructs a new `String` from the result and
/// returns it.
fn remove_trailing_slash(s: &str) -> String {
    if s.ends_with("/") {
        s[..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Session {
        fn some() -> Session {
            Session {
                id: Some(0),
                app_id: Some(String::from("app_id")),
                owner: Some(String::from("owner")),
                proxy_user: Some(String::from("proxy_user")),
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

    #[test]
    fn test_client_new() {
        let url = "http://example.com:8998";
        let client = Client::new(url);
        assert_eq!(url, client.url);
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
    fn test_remove_trailing_slash() {
        struct TestCase {
            s: &'static str,
            expected: String,
        }

        let test_cases = vec![
            TestCase {
                s: "http://example.com/",
                expected: "http://example.com".to_string(),
            },
            TestCase {
                s: "http://example.com",
                expected: "http://example.com".to_string(),
            },
        ];

        for test_case in test_cases {
            assert_eq!(test_case.expected, remove_trailing_slash(test_case.s));
        }
    }
}
