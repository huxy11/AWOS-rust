use std::fmt;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct HttpError(u16);

impl From<u16> for HttpError {
    fn from(code: u16) -> Self {
        HttpError(code)
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.0, self.info())
    }
}
impl fmt::Debug for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // fmt::Debug::fmt(&self.0, f)
        f.debug_map().entry(&self.0, &self.info()).finish()
    }
}

macro_rules! http_error_defines {
    (
        $(
            ($num:expr, $const_name:ident, $literal:expr);
        )+
    ) => {
        impl HttpError {
        $(
            pub const $const_name: HttpError = HttpError($num);
        )+
        pub fn info(&self) -> &'static str {
            match self.0 {
                $(
                $num => $literal,
                )+
                _ => panic!("Unknown http error code")
            }
        }
        }
    }
}

http_error_defines! {
    (400, BAD_REQUEST, "Bad Request");
    (401, UNAUTHORIZED, "Unauthorized");
    (402, PAYMENT_REQUIRED, "Payment Required");
    (403, FORBIDDEN, "Forbidden");
    (404, NOT_FOUND, "Not Found");
    (405, METHOD_NOT_ALLOWED, "Method Not Allowed");
    (406, NOT_ACCEPTABLE, "Not Acceptable");
    (407, PROXY_AUTHENTICATION_REQUIRED, "Proxy Authentication Required");
    (408, REQUEST_TIMEOUT, "Request Timeout");
    (409, CONFLICT, "Conflict");
    (410, GONE, "Gone");
    (411, LENGTH_REQUIRED, "Length Required");
    (412, PRECONDITION_FAILED, "Precondition Failed");
    (413, PAYLOAD_TOO_LARGE, "Payload Too Large");
    (414, URI_TOO_LONG, "URI Too Long");
    (415, UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type");
    (416, RANGE_NOT_SATISFIABLE, "Range Not Satisfiable");
    (417, EXPECTATION_FAILED, "Expectation Failed");
    (418, IM_A_TEAPOT, "I'm a teapot");
    (421, MISDIRECTED_REQUEST, "Misdirected Request");
    (422, UNPROCESSABLE_ENTITY, "Unprocessable Entity");
    (423, LOCKED, "Locked");
    (424, FAILED_DEPENDENCY, "Failed Dependency");
    (426, UPGRADE_REQUIRED, "Upgrade Required");
    (428, PRECONDITION_REQUIRED, "Precondition Required");
    (429, TOO_MANY_REQUESTS, "Too Many Requests");
    (431, REQUEST_HEADER_FIELDS_TOO_LARGE, "Request Header Fields Too Large");
    (451, UNAVAILABLE_FOR_LEGAL_REASONS, "Unavailable For Legal Reasons");
    (500, INTERNAL_SERVER_ERROR, "Internal Server Error");
    (501, NOT_IMPLEMENTED, "Not Implemented");
    (502, BAD_GATEWAY, "Bad Gateway");
    (503, SERVICE_UNAVAILABLE, "Service Unavailable");
    (504, GATEWAY_TIMEOUT, "Gateway Timeout");
    (505, HTTP_VERSION_NOT_SUPPORTED, "HTTP Version Not Supported");
    (506, VARIANT_ALSO_NEGOTIATES, "Variant Also Negotiates");
    (507, INSUFFICIENT_STORAGE, "Insufficient Storage");
    (508, LOOP_DETECTED, "Loop Detected");
    (510, NOT_EXTENDED, "Not Extended");
    (511, NETWORK_AUTHENTICATION_REQUIRED, "Network Authentication Required");
}

#[cfg(test)]
#[test]
fn http_error_test() {
    let not_found = HttpError::NOT_FOUND;
    println!("{}", not_found);
    println!("{:#?}", not_found);
}
