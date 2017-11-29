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

    /// Gets information of a single session.
    pub fn get_session(session_id: i64) -> Result<Session, &'static str> {
        Ok(Session {
            id: Some(0),
            app_id: Some(String::from("")),
            owner: Some(String::from("")),
            proxy_user: Some(String::from("")),
            kind: Some(SessionKind::Spark),
            log: Some(Vec::new()),
            state: Some(SessionState::NotStarted),
            app_info: Some(HashMap::new()),
        })
    }
}

/// Session which represents an interactive shell.
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
#[derive(Debug, PartialEq)]
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
#[derive(Debug, PartialEq)]
pub enum SessionKind {
    Spark,
    Pyspark,
    Pyspark3,
    Sparkr,
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
    fn client_new() {
        let base_url = "http://example.com:8998/";
        let client = Client::new(base_url);
        assert_eq!(base_url, client.base_url);
    }

    #[test]
    fn session_id() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.id, session.id());
        }
    }

    #[test]
    fn session_app_id() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.app_id.as_ref().map(String::as_str), session.app_id());
        }
    }

    #[test]
    fn session_owner() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.owner.as_ref().map(String::as_str), session.owner());
        }
    }

    #[test]
    fn session_proxy_user() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.proxy_user.as_ref().map(String::as_str), session.proxy_user());
        }
    }

    #[test]
    fn session_kind() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.kind.as_ref(), session.kind());
        }
    }

    #[test]
    fn session_log() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.log.as_ref(), session.log());
        }
    }

    #[test]
    fn session_state() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.state.as_ref(), session.state());
        }
    }

    #[test]
    fn session_app_info() {
        for session in vec![Session::some(), Session::none()] {
            assert_eq!(session.app_info.as_ref(), session.app_info());
        }
    }
}
