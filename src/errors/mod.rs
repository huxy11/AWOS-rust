mod awos_errors;
mod http_errors;

pub use awos_errors::Error;
pub use http_errors::HttpError;

pub type Result<T> = std::result::Result<T, Error>;
