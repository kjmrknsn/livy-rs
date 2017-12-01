use http;
use http::Method;
use serde::de::DeserializeOwned;

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
    pub fn send<T: DeserializeOwned>(&self, method: Method, path: &str) -> Result<T, String> {
        http::send(method,
                   format!("{}{}", self.url, path).as_str(),
                   self.gssnegotiate.as_ref(),
                   self.username.as_ref().map(String::as_ref))
    }
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
