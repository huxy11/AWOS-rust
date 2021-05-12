use oss_sdk::OSSError;
use quick_xml::Error as QxmlError;
use rusoto_core::{request::BufferedHttpResponse, RusotoError};
use rusoto_s3::{
    DeleteObjectError, GetObjectError, HeadObjectError, ListObjectsError, PutObjectError,
};
use std::{error::Error as StdError, io::ErrorKind, str::Utf8Error, string::FromUtf8Error};

use super::IoError;

#[derive(Debug, Display)]
pub enum Error {
    /// An error occurs dispatching the HTTP request. Converted to IoError for convenience.
    Io(IoError),
    /// A service-denpended error, generally occurs during configuring.
    Service,
    /// An error occurs when parsing the response, such as constructing a String from UTF8.
    Parse(ParseError),
    /// An error message  from one of our underlying modules. Wrapped up to gracefully handling it.
    #[display(fmt = "{}", msg)]
    Internal { msg: String },
}

impl Error {
    /// Returns None if self is NOT an IO Error
    pub fn io_kind(&self) -> Option<ErrorKind> {
        if let Error::Io(_io_error) = self {
            Some(_io_error.kind())
        } else {
            None
        }
    }
}

#[derive(Debug, Display)]
pub enum ParseError {
    UTF8(Utf8Error),
    InvalidFormat { msg: String },
}

fn to_error<E>(e: RusotoError<E>) -> Error {
    match e {
        RusotoError::Blocking => Error::Internal {
            msg: "attempting to run a future as blocking".to_string(),
        },
        RusotoError::Credentials(_internal) => Error::Internal {
            msg: _internal.message,
        },
        RusotoError::HttpDispatch(_internal) => Error::Internal {
            msg: format!("{}", _internal),
        },
        RusotoError::ParseError(_msg) => Error::Internal { msg: _msg },
        RusotoError::Unknown(_http_response) => _http_response.into(),
        RusotoError::Validation(_msg) => Error::Internal { msg: _msg },
        _ => panic!("Should Not Reach Here"),
    }
}
impl From<BufferedHttpResponse> for Error {
    fn from(resp: BufferedHttpResponse) -> Self {
        match resp.status.as_u16() {
            404 => Error::Io(IoError::from(ErrorKind::NotFound)),
            403 => Error::Io(IoError::from(ErrorKind::PermissionDenied)),
            408 => Error::Io(IoError::from(ErrorKind::TimedOut)),
            _ => Error::Internal {
                msg: format!(
                    "Unknown Error. Http Response is: StatusCode:{}, Headers:{:?}, Body:{}",
                    resp.status,
                    resp.headers,
                    resp.body_as_str(),
                ),
            },
        }
    }
}
impl From<u16> for Error {
    fn from(status_code: u16) -> Self {
        match status_code {
            404 => Error::Io(IoError::from(ErrorKind::NotFound)),
            403 => Error::Io(IoError::from(ErrorKind::PermissionDenied)),
            _ => panic!("Should Not from this number {}", status_code),
        }
    }
}
impl From<RusotoError<ListObjectsError>> for Error {
    fn from(e: RusotoError<ListObjectsError>) -> Self {
        match e {
            RusotoError::Service(ListObjectsError::NoSuchBucket(_msg)) => {
                Error::Io(IoError::new(ErrorKind::NotFound, _msg))
            }
            _ => to_error(e),
        }
    }
}
impl From<RusotoError<GetObjectError>> for Error {
    fn from(e: RusotoError<GetObjectError>) -> Self {
        match e {
            RusotoError::Service(GetObjectError::NoSuchKey(msg)) => {
                Error::Io(IoError::new(ErrorKind::NotFound, msg))
            }
            RusotoError::Service(GetObjectError::InvalidObjectState(msg)) => {
                Error::Io(IoError::new(ErrorKind::Other, msg))
            }
            _ => to_error(e),
        }
    }
}
impl From<RusotoError<HeadObjectError>> for Error {
    fn from(e: RusotoError<HeadObjectError>) -> Self {
        match e {
            RusotoError::Service(HeadObjectError::NoSuchKey(msg)) => {
                Error::Io(IoError::new(ErrorKind::NotFound, msg))
            }
            _ => to_error(e),
        }
    }
}
impl From<RusotoError<PutObjectError>> for Error {
    fn from(e: RusotoError<PutObjectError>) -> Self {
        to_error(e)
    }
}
impl From<RusotoError<DeleteObjectError>> for Error {
    fn from(e: RusotoError<DeleteObjectError>) -> Self {
        to_error(e)
    }
}

impl From<OSSError> for Error {
    fn from(e: OSSError) -> Self {
        Error::Internal {
            msg: format!("{:?}", e),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::Parse(ParseError::UTF8(e.utf8_error()))
    }
}
impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Self {
        Error::Parse(ParseError::UTF8(e))
    }
}

impl From<QxmlError> for Error {
    fn from(e: QxmlError) -> Self {
        Error::Internal { msg: e.to_string() }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Io(_e) => Some(_e),
            Error::Parse(_e) => _e.source(),
            _ => None,
        }
    }
}

impl StdError for ParseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ParseError::UTF8(_e) => Some(_e),
            _ => None,
        }
    }
}
