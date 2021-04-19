use oss_sdk::HttpError;
use quick_xml::Error as QxmlError;
 
use rusoto_core::region::ParseRegionError;
use std::{error::Error as StdError, str::ParseBoolError};
use std::{io::Error as IoError, str::Utf8Error, string::FromUtf8Error};

#[derive(Debug, Display)]
pub enum Error {
    Object(ObjectError),
    Io(IoError),
    String(FromUtf8Error),
    Str(Utf8Error),
    HttpError(HttpError),
    ParseRegion(ParseRegionError),
    Qxml(QxmlError),
    ParseBool(ParseBoolError),
}

impl From<QxmlError> for Error {
    fn from(e: QxmlError) -> Error {
        Error::Qxml(e)
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        Error::Io(e)
    }
}

impl From<HttpError> for Error {
    fn from(e: HttpError) -> Error {
        Error::HttpError(e)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Error {
        Error::String(e)
    }
}
impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Error {
        Error::Str(e)
    }
}

impl From<ParseBoolError> for Error {
    fn from(e: ParseBoolError) -> Error {
        Error::ParseBool(e)
    }
}
impl From<ParseRegionError> for Error {
    fn from(e: ParseRegionError) -> Error {
        Error::ParseRegion(e)
    }
}
#[derive(Debug, Display)]
pub enum ObjectError {
    #[display(fmt = "PUT ERROR: {:#?}", msg)]
    PutError { msg: String },
    #[display(fmt = "GET ERROR: {:#?}", msg)]
    GetError { msg: String },
    #[display(fmt = "DELETE ERROR: {:#?}", msg)]
    DeleteError { msg: String },
    #[display(fmt = "HEAD ERROR: {:#?}", msg)]
    HeadError { msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl StdError for Error {}
