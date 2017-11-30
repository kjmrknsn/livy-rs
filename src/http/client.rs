use http::XRequestedBy;
use hyper::header::ContentType;
use reqwest;
use serde::de::DeserializeOwned;

/// Basic HTTP client
pub struct BasicClient {
    client: reqwest::Client,
}

impl BasicClient {
    /// Constructs a new `Client`.
    ///
    /// # Examples
    /// ```
    /// use livy::http::client::BasicClient;
    ///
    /// let client = BasicClient::new();
    /// ```
    pub fn new() -> BasicClient {
        BasicClient {
            client: reqwest::Client::new(),
        }
    }

    /// Sends an HTTP GET request to `url`, deserializes the response body and
    /// returns the result.
    pub fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        let mut res = match self.client.get(url).header(ContentType::json()).send() {
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

    /// Sends an HTTP POST request to `url`, deserializes the response body and
    /// returns the result.
    pub fn post<T: DeserializeOwned>(&self, url: &str, body: String) -> Result<T, String> {
        let mut res = match self.client.post(url)
            .header(ContentType::json())
            .header(XRequestedBy("x".to_owned()))
            .body(body)
            .send() {
            Ok(res) => res,
            Err(err) => return Err(format!("{}", err))
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

    /// Sends an HTTP DELETE request to `url`.
    pub fn delete(&self, url: &str) -> Result<(), String> {
        let res = match self.client.delete(url)
            .header(ContentType::json())
            .header(XRequestedBy("x".to_owned()))
            .send() {
            Ok(res) => res,
            Err(err) => return Err(format!("{}", err))
        };

        if res.status() != reqwest::StatusCode::Ok {
            return Err(format!("invalid status code: {}", res.status()));
        }

        Ok(())
    }
}
