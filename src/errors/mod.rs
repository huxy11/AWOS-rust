mod awos_errors;
// mod http_errors;

pub use awos_errors::Error;
pub use awos_errors::ParseError;
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) use std::io::Error as IoError;
