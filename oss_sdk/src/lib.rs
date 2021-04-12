#[macro_use]
extern crate log;

#[macro_use]
extern crate derive_more;

mod http_client;
mod oss;
mod regions;
mod schema;

pub use oss::OSS_PREFIX;

pub use crate::http_client::{HttpError, HttpResponse, SignAndDispatch};
pub use crate::oss::OSSClient;
pub use crate::regions::Region;

pub type DefaultOssclient = OSSClient<reqwest::blocking::Client>;

impl DefaultOssclient {
    pub fn new_oss_cli<'a, R, S, B, S1, S2>(
        region: R,
        schema: S,
        bucket: B,
        access_key_id: S1,
        access_key_secret: S2,
    ) -> Self
    where
        R: AsRef<str>,
        S: Into<Option<&'a str>>,
        B: Into<Option<&'a str>>,
        S1: Into<String>,
        S2: Into<String>,
    {
        Self::new(
            reqwest::blocking::Client::new(),
            region,
            schema,
            bucket,
            access_key_id,
            access_key_secret,
        )
    }
}
