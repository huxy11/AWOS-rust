use super::*;
use base64::encode;
use chrono::Utc;

use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha1::Sha1;

const RESOURCES: [&str; 51] = [
    "acl",
    "uploads",
    "location",
    "cors",
    "logging",
    "website",
    "referer",
    "lifecycle",
    "delete",
    "append",
    "tagging",
    "objectMeta",
    "uploadId",
    "partNumber",
    "security-token",
    "position",
    "img",
    "style",
    "styleName",
    "replication",
    "replicationProgress",
    "replicationLocation",
    "cname",
    "bucketInfo",
    "comp",
    "qos",
    "live",
    "status",
    "vod",
    "startTime",
    "endTime",
    "symlink",
    "x-oss-process",
    "response-content-type",
    "response-content-language",
    "response-expires",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "udf",
    "udfName",
    "udfImage",
    "udfId",
    "udfImageDesc",
    "udfApplication",
    "comp",
    "udfApplicationLog",
    "restore",
    "callback",
    "callback-var",
    "continuation-token",
];

impl<'a> SignedRequest<'a> {
    pub(crate) fn oss_sign(&mut self) {
        self.add_header("date", Utc::now().format("%a, %d %b %Y %T GMT").to_string());
        let auth_header = self.authorization_header();
        self.add_header(auth_header.0, auth_header.1);
    }
    fn authorization_header(&self) -> (&'static str, String) {
        let headers = &self.headers;
        let date = headers
            .get("date")
            .map(|val| val.as_str())
            .unwrap_or_default();
        let content_type = headers
            .get("content_type")
            .map(|val| val.as_str())
            .unwrap_or_default();

        let content_md5 = headers
            .get("Content-MD5")
            .map(|val| encode(val.as_str()))
            .unwrap_or_default();

        let mut oss_headers_str = String::new();
        for (k, v) in headers
            .iter()
            .filter(|(k, _)| k.as_str().contains("x-oss-"))
        {
            oss_headers_str += &format!("{}:{}\n", k, v);
        }

        let oss_resource_str = get_oss_resource_str(&self.bucket, &self.object, &self.params);
        let sign_str = format!(
            "{}\n{}\n{}\n{}\n{}{}",
            &self.method, content_md5, content_type, date, oss_headers_str, oss_resource_str
        );

        let mut hasher = Hmac::new(Sha1::new(), &self.access_key_secret.as_bytes());
        hasher.input(sign_str.as_bytes());
        let sign_str_base64 = encode(hasher.result().code());

        let authorization = format!("OSS {}:{}", &self.access_key_id, sign_str_base64);
        ("Authorization", authorization)
    }
}

#[inline]
fn get_resources_str(params: &Params) -> String {
    let mut resources: Vec<(String, Option<String>)> = params
        .iter()
        .filter(|(k, _)| RESOURCES.contains(&k.as_str()))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    resources.sort_by(|a, b| a.0.cmp(&b.0));
    let mut result = String::new();
    for (k, v) in resources {
        if result.is_empty() {
            result += "?";
        } else {
            result += "&";
        }
        if let Some(vv) = v {
            result += &format!("{}={}", k, vv);
        } else {
            result += &k;
        }
    }
    result
}

#[inline]
fn get_oss_resource_str(bucket: &str, object: &str, params: &Params) -> String {
    let oss_resources = get_resources_str(params);
    if bucket == "" {
        format!("/{}{}", object, oss_resources)
    } else {
        format!("/{}/{}{}", bucket, object, oss_resources)
    }
}
