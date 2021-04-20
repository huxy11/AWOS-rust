use oss_sdk::OSSError;
use quick_xml::Error as QxmlError;
use rusoto_core::region::ParseRegionError;
use std::{error::Error as StdError, str::ParseBoolError};
use std::{io::Error as IoError, str::Utf8Error};

use super::http_errors::HttpError;

#[derive(Debug, Display)]
pub enum Error {
    Http(HttpError),
    Io(IoError),
    /// 将Utf8转换成String时可能出现的错误
    StrConvert(Utf8Error),
    OSS(OSSError),
    #[display(fmt = "Aws S3 ERROR")]
    // AWS(S3Error),
    AWS(S3Error),
    ParseRegion(ParseRegionError),
    Qxml(QxmlError),
    ParseBool(ParseBoolError),
}

#[derive(Debug, Display)]
pub enum S3Error {
    #[display(fmt = "PUT ERROR: {:#?}", msg)]
    PutError { msg: String },
    #[display(fmt = "GET ERROR: {:#?}", msg)]
    GetError { msg: String },
    #[display(fmt = "DELETE ERROR: {:#?}", msg)]
    DeleteError { msg: String },
    #[display(fmt = "HEAD ERROR: {:#?}", msg)]
    HeadError { msg: String },
}

/* for Convenient Http check */
macro_rules! is_http_errors {
    (
        $(
            ($fn_name:ident, $const_name:ident);
        )+
    ) => {
        impl Error {
        $(
            pub fn $fn_name(&self) -> bool {
                if let Error::Http(HttpError::$const_name) = self {
                    true
                } else {
                    false
                }
            }
        )+
        }
    }
}

/* 有需要添加方法的话，在此按着格式添加就好 */
is_http_errors! {
    (is_bad_request, BAD_REQUEST);
    (is_not_found, NOT_FOUND);
    (is_forbidden, FORBIDDEN);
    (is_interal_server_error, INTERNAL_SERVER_ERROR);
}

/* From trait Implements*/
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

impl From<OSSError> for Error {
    fn from(e: OSSError) -> Error {
        Error::OSS(e)
    }
}
impl From<S3Error> for Error {
    fn from(e: S3Error) -> Error {
        Error::AWS(e)
    }
}

impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Error {
        Error::StrConvert(e)
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

impl StdError for Error {}
