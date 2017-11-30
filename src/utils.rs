use std::fmt::Display;

/// Constructs a new `String` which represents a key-value
/// parameter string from `key` and `value` and returns the
/// result as a form of `Some(String)`
///
/// Returns `None` if `value` is `None`.
///
/// # Examples
/// ```
/// use livy::utils;
///
/// assert_eq!(Some("from=2".to_string()), utils::param("from", Some(2)));
/// assert_eq!(None, utils::param::<i32>("from", None));
/// ```
pub fn param<T: Display>(key: &str, value: Option<T>) -> Option<String> {
    match value {
        Some(value) => Some(format!("{}={}", key, value)),
        None => None
    }
}

/// Removes the trailing slash of `s` if it exists,
/// constructs a new `String` from the result and
/// returns it.
///
/// # Examples
/// ```
/// use livy::utils;
///
/// assert_eq!("http://example.com".to_string(),
///            utils::remove_trailing_slash("http://example.com/"));
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

