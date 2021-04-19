#[macro_use]
extern crate derive_more;

mod awos;
mod aws;
mod errors;
mod inner_client;
mod oss;
mod types;

use errors::*;
use rusoto_s3::S3Client as S3Inner;

pub use awos::*;
pub use errors::*;
pub use oss_sdk::*;
pub use types::*;
