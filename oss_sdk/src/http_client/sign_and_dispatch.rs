use std::{ str::FromStr};

use reqwest::{header::HeaderName, Method};

use super::{errors::DispatchError, responses::HttpResponse, SignedRequest};

use async_trait::async_trait;

#[async_trait]
pub trait SignAndDispatch {
    async fn sign_and_dispatch(
        &self,
        mut request: SignedRequest,
        // timeout: Option<Duration>,
    ) -> Result<HttpResponse, DispatchError>;
    // ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, DispatchError>> + Send>>;
}

#[async_trait]
impl SignAndDispatch for reqwest::Client {
    async fn sign_and_dispatch(
        &self,
        mut request: SignedRequest,
        // timeout: Option<Duration>,
    ) -> Result<HttpResponse, DispatchError> {
        // ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, DispatchError>> + Send>> {
        request.oss_sign();
        let url = request.generate_url();
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, val) in request.headers.iter() {
            headers.insert(HeaderName::from_bytes(key.as_bytes())?, val.parse()?);
        }
        let method = Method::from_str(request.method).map_err(|_| DispatchError::InvalidMethod)?;
        let mut request_builder = self
            .request(method, &url)
            .headers(headers)
            .query(&request.params);
        if let Some(_payload) = request.payload {
            request_builder = request_builder.body(_payload.into_vec());
        }
        let ret = request_builder.send();
        let http_resp = HttpResponse::from_resp(ret.await?).await;
        Ok(http_resp)
    }
}

// impl From<reqwest::Response> for HttpResponse {
//     fn from(resp: reqwest::Response) -> Self {
//         let status = resp.status();
//         let headers = resp.headers().to_owned();
//         // let bytes = rt.block_on(resp.bytes()).unwrap_or_default();
//         // let bytes = Byt;
//         resp.bytes()

//         Self {
//             status,
//             headers,
//             body: Box::pin(bytes),
//         }
//     }
// }
