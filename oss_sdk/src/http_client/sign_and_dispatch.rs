use std::str::FromStr;

use reqwest::{header::HeaderName, Method};

use super::{errors::HttpError, responses::HttpResponse, SignedRequest};

pub trait SignAndDispatch {
    fn sign_and_dispatch(
        &self,
        request: SignedRequest,
        // timeout: Option<Duration>,
    ) -> Result<HttpResponse, HttpError>;
}

impl SignAndDispatch for reqwest::blocking::Client {
    fn sign_and_dispatch(
        &self,
        mut request: SignedRequest,
        // timeout: Option<Duration>,
    ) -> Result<HttpResponse, HttpError> {
        request.oss_sign();
        let url = request.generate_url();
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, val) in request.headers.iter() {
            headers.insert(HeaderName::from_bytes(key.as_bytes())?, val.parse()?);
        }
        let method = Method::from_str(request.method).map_err(|_| HttpError::InvalidMethod)?;
        let mut request_builder = self
            .request(method, &url)
            .headers(headers)
            .query(&request.params);
        if let Some(_payload) = request.payload {
            request_builder = request_builder.body(_payload.into_vec());
        }
        Ok(HttpResponse::from(request_builder.send()?))
    }
}

impl From<reqwest::blocking::Response> for HttpResponse {
    fn from(resp: reqwest::blocking::Response) -> Self {
        Self {
            status: resp.status(),
            headers: resp.headers().to_owned(),
            body: Box::pin(resp.bytes().unwrap_or_default()),
        }
    }
}
