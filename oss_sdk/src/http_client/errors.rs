/// Generic error type returned by all http requests.
#[derive(Debug, PartialEq, Display)]
pub enum HttpError {
    InvalidMethod,
    HeaderError(String),
    DispatchError(String),
    Unknown(String),
    // /// A service-specific error occurred.
    // Service(E),
    // /// An error occurred dispatching the HTTP request
    // HttpDispatch(HttpDispatchError),
    // /// An error was encountered with AWS credentials.
    // Credentials(CredentialsError),
    // /// A validation error occurred.  Details from AWS are provided.
    // Validation(String),
    // /// An error occurred parsing the response payload.
    // ParseError(String),
    // /// An unknown error occurred.  The raw HTTP response is provided.
    // Unknown(BufferedHttpResponse),
    // /// An error occurred when attempting to run a future as blocking
    // Blocking,
}

impl From<reqwest::Error> for HttpError {
    fn from(e: reqwest::Error) -> Self {
        Self::DispatchError(e.to_string())
    }
}
impl From<reqwest::header::InvalidHeaderName> for HttpError {
    fn from(e: reqwest::header::InvalidHeaderName) -> Self {
        let mut s = "InvalidKey".to_string();
        s.push_str(&e.to_string());
        Self::HeaderError(s)
    }
}
impl From<reqwest::header::InvalidHeaderValue> for HttpError {
    fn from(e: reqwest::header::InvalidHeaderValue) -> Self {
        let mut s = "InvalidValue".to_string();
        s.push_str(&e.to_string());
        Self::HeaderError(s)
    }
}

// impl std::error::Error for HttpError {}
