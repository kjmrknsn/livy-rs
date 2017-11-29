use reqwest;
use serde::de::DeserializeOwned;

/// HTTP client
pub struct Client {
    client: reqwest::Client,
}

impl Client {
    /// Constructs a new `Client`.
    ///
    /// # Examples
    /// ```
    /// use livy::http::Client;
    ///
    /// let client = Client::new();
    /// ```
    pub fn new() -> Client {
        Client {
            client: reqwest::Client::new(),
        }
    }

    /// Sends an HTTP GET request to `url`, deserializes the response body and
    /// returns the result.
    pub fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        let mut res = match self.client.get(url).send() {
            Ok(res) => res,
            Err(err) => return Err(format!("{}", err)),
        };

        if res.status() != reqwest::StatusCode::Ok {
            return Err(format!("invalid status code: {}", res.status()));
        }

        let res: reqwest::Result<T> = res.json();

        match res {
            Ok(res) => Ok(res),
            Err(err) => Err(format!("{}", err)),
        }
    }
}
