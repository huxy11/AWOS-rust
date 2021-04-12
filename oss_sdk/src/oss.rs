use base64::encode;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha1::Sha1;

use std::collections::BTreeMap;

use crate::{
    http_client::{HttpError, HttpResponse, SignAndDispatch, SignedRequest},
    regions::Region,
    schema::Schema,
};

pub const OSS_PREFIX: &str = "x-oss-meta-";
pub const OSS_CANONOCALIZED_PREFIX: &str = "x-oss-";
const CONTENT_TYPE: &str = "content-type";
const CONTENT_MD5: &str = "Content-MD5";

#[derive(Debug)]
pub struct OSSClient<C: SignAndDispatch> {
    pub client: C,
    pub region: Region,
    access_key_id: String,
    access_key_secret: String,
    bucket: String,
    schema: Schema,
}

impl<C: SignAndDispatch> OSSClient<C> {
    pub fn new<'a, R, S, B, S1, S2>(
        client: C,
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
        OSSClient {
            client,
            region: region.as_ref().parse().unwrap_or_default(),
            schema: schema
                .into()
                .and_then(|_schema| _schema.parse().ok())
                .unwrap_or_default(),
            bucket: bucket.into().unwrap_or_default().to_string(),
            access_key_id: access_key_id.into(),
            access_key_secret: access_key_secret.into(),
        }
    }
    pub fn get_access_key(&self) -> (&str, &str) {
        (&self.access_key_id, &self.access_key_secret)
    }
    pub fn get_request<'a, S>(&self, object: S) -> SignedRequest
    where
        S: Into<Option<&'a str>>,
    {
        self.generate_request("GET", object.into().unwrap_or_default(), None)
    }
    pub fn put_request<'a, S, P>(&self, object: S, payload: P) -> SignedRequest
    where
        S: Into<String>,
        P: Into<Option<Box<[u8]>>>,
    {
        self.generate_request("PUT", object, payload)
    }
    pub fn head_request<S>(&self, object: S) -> SignedRequest
    where
        S: Into<String>,
    {
        self.generate_request("HEAD", object, None)
    }
    pub fn del_request<S>(&self, object: S) -> SignedRequest
    where
        S: Into<String>,
    {
        self.generate_request("DELETE", object, None)
    }
    pub fn sign_and_dispatch(&self, request: SignedRequest) -> Result<HttpResponse, HttpError> {
        self.client.sign_and_dispatch(request)
    }
    pub fn get_signed_url<'a, H>(
        &self,
        object: &str,
        verb: &str,
        expires: u64,
        params: &str,
        headers: H,
    ) -> String
    where
        H: Into<Option<BTreeMap<&'a str, &'a str>>>,
    {
        let mut content_type = "";
        let mut content_md5 = "";
        let mut oss_headers_str = String::new();
        if let Some(_headers) = headers.into() {
            for (k, v) in _headers {
                if k.starts_with(OSS_CANONOCALIZED_PREFIX) {
                    oss_headers_str += k;
                    oss_headers_str += ":";
                    oss_headers_str += v;
                    oss_headers_str += "\n";
                } else if k == CONTENT_TYPE {
                    content_type = v;
                } else if k == CONTENT_MD5 {
                    content_md5 = v;
                }
            }
        }
        let oss_resource_str = get_oss_subresource_signed_str(&self.bucket, object, params);
        let sign_str = format!(
            "{}\n{}\n{}\n{}\n{}{}",
            verb, content_md5, content_type, expires, oss_headers_str, oss_resource_str
        );
        let mut hasher = Hmac::new(Sha1::new(), self.access_key_secret.as_bytes());
        hasher.input(sign_str.as_bytes());
        let sign_str_base64 = encode(hasher.result().code());

        let auth_params = format!(
            "OSSAccessKeyId={}&Expires={}&Signature={}",
            self.access_key_id, expires, sign_str_base64
        );
        self.host(object, &auth_params)
    }

    fn generate_request<'a, S1, P>(
        &self,
        method: &'static str,
        object: S1,
        payload: P,
    ) -> SignedRequest
    where
        S1: Into<String>,
        P: Into<Option<Box<[u8]>>>,
    {
        let mut signed_rqst = SignedRequest::new(
            method,
            &self.region,
            &self.bucket,
            object,
            &self.access_key_id,
            &self.access_key_secret,
            self.schema,
        );
        let content_length = if let Some(_payload) = payload.into() {
            signed_rqst.load(_payload.to_owned())
        } else {
            0
        };
        signed_rqst.add_header("content-length", content_length.to_string());
        signed_rqst
    }
    fn host(&self, object: &str, params: &str) -> String {
        format!(
            "{}://{}.{}/{}?{}",
            self.schema,
            self.bucket,
            self.region.endpoint(),
            object,
            params,
        )
    }
}

impl<'a> SignedRequest<'a> {
    fn _add_meta<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        let mut key_lower = key.into();
        key_lower.make_ascii_lowercase();
        if !key_lower.starts_with(OSS_PREFIX) {
            key_lower.insert_str(0, OSS_PREFIX)
        }
        self.add_header(key_lower, value);
    }
    pub fn add_meta<K, V, M>(&mut self, meta: M)
    where
        K: Into<String>,
        V: Into<String>,
        M: IntoIterator<Item = (K, V)>,
    {
        for (k, v) in meta {
            self._add_meta(k, v);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    const FILE_NAME: &str = "rust_oss_sdk_test";
    const BUF: &[u8] = "This is just a put test".as_bytes();

    #[test]
    fn smoke_test() {
        let bucket = std::env::var("OSS_BUCKET").unwrap();
        let access_key_id = std::env::var("OSS_KEY_ID").unwrap();
        let access_key_secret = std::env::var("OSS_KEY_SECRET").unwrap();
        let oss_instance = OSSClient::new(
            reqwest::blocking::Client::new(),
            "北京",
            None,
            bucket.as_ref(),
            access_key_id,
            access_key_secret,
        );

        let mut rqst = oss_instance.put_request(FILE_NAME, BUF.to_vec().into_boxed_slice());
        rqst.add_meta([("test-key", "test-val")].iter().map(|a| a.to_owned()));
        let ret = oss_instance.sign_and_dispatch(rqst);
        assert!(ret.is_ok() && ret.unwrap().status.is_success());

        let mut rqst = oss_instance.get_request(None);
        rqst.add_params("prefix", "rust_oss_sdk");
        let ret = oss_instance.sign_and_dispatch(rqst);
        assert!(ret.is_ok() && ret.unwrap().status.is_success());

        let rqst = oss_instance.get_request(FILE_NAME);
        let ret = oss_instance.sign_and_dispatch(rqst);
        assert!(ret.is_ok() && ret.unwrap().body == Box::pin(BUF));

        let rqst = oss_instance.head_request(FILE_NAME);
        let ret = oss_instance.sign_and_dispatch(rqst);
        assert!(ret.is_ok() && ret.unwrap().headers.contains_key("x-oss-meta-test-key"));

        let rqst = oss_instance.del_request(FILE_NAME);
        let ret = oss_instance.sign_and_dispatch(rqst);
        assert!(ret.is_ok() && ret.unwrap().status.is_success());

        let rqst = oss_instance.get_request(FILE_NAME);
        let ret = oss_instance.sign_and_dispatch(rqst);
        assert!(ret.is_ok() && ret.unwrap().status.is_client_error());
    }
}
#[inline]
fn get_oss_subresource_signed_str(bucket: &str, object: &str, oss_resources: &str) -> String {
    let oss_resources = if oss_resources != "" {
        String::from("?") + oss_resources
    } else {
        String::new()
    };
    if bucket == "" {
        format!("/{}{}", bucket, oss_resources)
    } else {
        format!("/{}/{}{}", bucket, object, oss_resources)
    }
}
