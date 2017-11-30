/// HTTP client
pub mod client;

use std::fmt::Display;

header! { (XRequestedBy, "X-Requested-By") => [String] }

/// Constructs a new `String` which represents a key-value
/// parameter string from `key` and `value` and returns the
/// result as a form of `Some(String)`.
///
/// Returns `None` if `value` is `None`.
///
/// # Examples
/// ```
/// use livy::http;
///
/// assert_eq!(Some("from=2".to_string()), http::param("from", Some(2)));
/// assert_eq!(None, http::param::<i32>("from", None));
/// ```
pub fn param<T: Display>(key: &str, value: Option<T>) -> Option<String> {
    match value {
        Some(value) => Some(format!("{}={}", key, value)),
        None => None
    }
}

/// Constructs a new `String` which represents a key-value parameters
/// string as a form of `"?key1=value1&key2=value2&..."`.
///
/// Returns an empty string if there is no `Some(String)` value in `params`.
///
/// # Examples
/// ```
/// use livy::http;
///
/// assert_eq!("".to_string(),
///            http::params(vec![]));
/// assert_eq!("".to_string(),
///            http::params(vec![None]));
/// assert_eq!("?key1=value1",
///            http::params(vec![Some("key1=value1".to_string())]));
/// assert_eq!("?key1=value1",
///            http::params(vec![Some("key1=value1".to_string()),
///                               None]));
/// assert_eq!("?key1=value1",
///            http::params(vec![None,
///                              Some("key1=value1".to_string())]));
/// assert_eq!("?key1=value1&key2=value2",
///            http::params(vec![Some("key1=value1".to_string()),
///                              Some("key2=value2".to_string())]));
/// ```
pub fn params(params: Vec<Option<String>>) -> String {
    let mut s = String::new();

    for param in params {
        match param {
            Some(param) => {
                if s.is_empty() {
                    s.push('?');
                } else {
                    s.push('&');
                }

                s.push_str(param.as_str());
            },
            None => (),
        }
    }

    s
}

/// Removes the trailing slash of `s` if it exists,
/// constructs a new `String` from the result and
/// returns it.
///
/// # Examples
/// ```
/// use livy::http;
///
/// assert_eq!("http://example.com".to_string(),
///            http::remove_trailing_slash("http://example.com/"));
/// ```
pub fn remove_trailing_slash(s: &str) -> String {
    if s.ends_with("/") {
        s[..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param() {
        struct TestCase {
            key: &'static str,
            value: Option<i32>,
            expected: Option<String>,
        }

        let test_cases = vec![
            TestCase {
                key: "from",
                value: Some(2),
                expected: Some("from=2".to_string()),
            },
            TestCase {
                key: "from",
                value: None,
                expected: None,
            },
        ];

        for test_case in test_cases {
            assert_eq!(test_case.expected, param(test_case.key, test_case.value));
        }
    }

    #[test]
    fn test_params() {
        struct TestCase {
            params: Vec<Option<String>>,
            expected: String,
        }

        let test_cases = vec![
            TestCase {
                params: vec![],
                expected: "".to_string(),
            },
            TestCase {
                params: vec![None],
                expected: "".to_string(),
            },
            TestCase {
                params: vec![Some("key1=value1".to_string())],
                expected: "?key1=value1".to_string(),
            },
            TestCase {
                params: vec![Some("key1=value1".to_string()), None],
                expected: "?key1=value1".to_string(),
            },
            TestCase {
                params: vec![None, Some("key1=value1".to_string())],
                expected: "?key1=value1".to_string(),
            },
            TestCase {
                params: vec![Some("key1=value1".to_string()), Some("key2=value2".to_string())],
                expected: "?key1=value1&key2=value2".to_string(),
            },
        ];

        for test_case in test_cases {
            assert_eq!(test_case.expected, params(test_case.params));
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
