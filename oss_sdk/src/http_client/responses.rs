use std::pin::Pin;

use bytes::Bytes;
use http::{HeaderMap, StatusCode};

/// Stores the response from a HTTP request.
pub struct HttpResponse {
    /// Status code of HTTP Request
    pub status: StatusCode,
    /// Contents of Response
    pub body: Pin<Box<Bytes>>,
    /// Response headers
    pub headers: HeaderMap,
}

impl std::fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let mut content = String::new();
        // self.body.read_to_string(&mut content).unwrap_or_default();
        f.debug_struct("HttpResponse")
            .field("StatusCode", &self.status.as_str())
            .field("Content", &String::from_utf8(self.body.to_vec()))
            .field("Headers", &self.headers)
            .finish()
    }
}
