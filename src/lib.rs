#[macro_use]
extern crate derive_more;

mod awos;
mod aws;
mod errors;
mod inner_client;
mod oss;
mod prelude;
mod types;

use errors::*;
use oss_sdk::*;

// Api
pub use awos::*;
// Errors
pub use errors::*;
// Opts
pub use types::*;
