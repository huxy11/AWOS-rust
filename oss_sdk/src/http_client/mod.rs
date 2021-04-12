use std::collections::BTreeMap;

mod auth;
mod errors;
mod requests;
mod responses;
mod sign_and_dispatch;

pub use errors::HttpError;
pub use requests::SignedRequest;
pub use responses::HttpResponse;
pub use sign_and_dispatch::SignAndDispatch;

type Params = BTreeMap<String, Option<String>>;
type Headers = BTreeMap<String, String>;
