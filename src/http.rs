use reqwest;
use serde::de::DeserializeOwned;

/// HTTP client
pub struct Client {
    client: reqwest::Client,
}

impl Client {
    pub fn new() -> Client {
        Client {
            client: reqwest::Client::new(),
        }
    }

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
