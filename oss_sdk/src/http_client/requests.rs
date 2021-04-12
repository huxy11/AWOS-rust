use crate::{regions::Region, schema::Schema};

use super::*;

#[derive(Clone, Debug, Default)]
pub struct SignedRequest<'a> {
    pub method: &'static str,
    pub region: Region,
    pub bucket: String,
    pub object: String,
    pub headers: Headers,
    pub params: Params,
    pub payload: Option<Box<[u8]>>,
    pub access_key_id: &'a str,
    pub access_key_secret: &'a str,
    pub url: String,
    schema: Schema,
}
impl<'a> SignedRequest<'a> {
    pub fn new<S1, S2>(
        method: &'static str,
        region: &Region,
        bucket: S1,
        object: S2,
        access_key_id: &'a str,
        access_key_secret: &'a str,
        schema: Schema,
    ) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            method,
            region: region.clone(),
            access_key_id,
            access_key_secret,
            bucket: bucket.into(),
            object: object.into(),
            schema,
            ..Default::default()
        }
    }
    /// Headers are kept sorted by key name with in BTreeMap
    pub fn add_headers<K, V>(&mut self, headers: impl IntoIterator<Item = (K, V)>)
    where
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in headers.into_iter() {
            self.add_header(k, v);
        }
    }
    pub(crate) fn add_header<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        let mut key_lower = key.into();
        key_lower.make_ascii_lowercase();

        self.headers.insert(key_lower, value.into());
    }
    pub fn remove_header(&mut self, key: &str) {
        let key_lower = key.to_ascii_lowercase();
        self.headers.remove(&key_lower);
    }

    pub fn set_params(&mut self, params: Params) {
        self.params = params;
    }
    pub fn add_params<'b, K, V>(&mut self, key: K, val: V)
    where
        K: Into<String>,
        V: Into<Option<&'b str>>,
    {
        self.params
            .insert(key.into(), val.into().map(|s| s.to_owned()));
    }
    pub fn load<P>(&mut self, payload: P) -> usize
    where
        P: Into<Box<[u8]>>,
    {
        self.payload = Some(payload.into());
        self.payload.as_ref().unwrap().len()
    }
    pub fn unload(&mut self) -> Option<Box<[u8]>> {
        self.payload.take()
    }
    pub fn set_content_type(&mut self, content_type: String) {
        self.add_header("content_type", content_type)
    }
    pub fn set_schema<S: AsRef<str>>(&mut self, schema: S) {
        self.schema = schema.as_ref().parse().unwrap_or_default()
    }
    pub fn get_schema(&self) -> String {
        format!("{}", self.schema)
    }
    // /// Computes and sets the Content-MD5 header based on the current payload.
    // ///
    // /// Has no effect if the payload is not set, or is not a buffer. Will not
    // /// override an existing value for the `Content-MD5` header.
    // pub fn maybe_set_content_md5_header(&mut self) {
    //     if self.headers.contains_key("Content-MD5") {
    //         return;
    //     }
    //     if let Some(SignedRequestPayload::Buffer(ref payload)) = self.payload {
    //         let digest = Md5::digest(payload);
    //         self.add_header("Content-MD5", &base64::encode(&*digest));
    //     }
    // }

    pub fn generate_url(&self) -> String {
        if self.bucket.is_empty() {
            format!(
                "{}://{}/{}{}",
                self.get_schema(),
                self.region.endpoint(),
                self.object,
                get_params_str(&self.params),
            )
        } else {
            format!(
                "{}://{}.{}/{}{}",
                self.get_schema(),
                self.bucket,
                self.region.endpoint(),
                self.object,
                get_params_str(&self.params),
            )
        }
    }
}

fn get_params_str(params: &Params) -> String {
    let mut result = String::new();
    for (k, v) in params {
        if result.is_empty() {
            result += "?";
        } else {
            result += "&";
        }
        if let Some(_v) = v {
            result += &format!("{}={}", k, _v);
        } else {
            result += k;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_request_test() {
        let sr = SignedRequest::new(
            "GET",
            &Region::BeiJing,
            "dev-sheet-calced",
            "A",
            "",
            "",
            Schema::Http,
        );
        println!("{}", sr.generate_url());
    }
}
