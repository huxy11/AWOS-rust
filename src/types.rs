use bytes::Bytes;
use std::{collections::HashMap, iter::FromIterator, pin::Pin, time::SystemTime};

use oss_sdk::{HttpResponse, OSS_PREFIX};

/// Response to Get, content encoded as String.
#[derive(Clone, Debug, Default)]
pub struct GetResp {
    pub content: String,
    pub meta: HashMap<String, String>,
    pub headers: HashMap<String, String>,
}
/// Response to GetAsBuffer, content as Bytes, the same as &[u8].
#[derive(Clone, Debug)]
pub struct GetAsBufferResp {
    pub content: Pin<Box<Bytes>>,
    pub meta: HashMap<String, String>,
    pub headers: HashMap<String, String>,
}

/// Response to ListDetails
#[derive(Clone, Debug, Default)]
pub struct ListDetailsResp {
    pub is_truncated: bool,
    pub objects: Vec<ObjectDetails>,
    pub prefixes: Vec<String>,
    pub next_marker: String,
}
#[derive(Debug, Clone, Default)]
pub struct ObjectDetails {
    pub key: String,
    pub last_modified: String,
    pub e_tag: String,
    pub size: String,
}

// For Convenience
impl From<HttpResponse> for GetAsBufferResp {
    fn from(resp: HttpResponse) -> Self {
        let mut meta = HashMap::new();
        let mut headers = HashMap::new();
        let content = resp.body;
        for (k, v) in resp.headers {
            if let Some(_name) = k {
                if _name.as_str().starts_with(OSS_PREFIX) {
                    meta.insert(
                        // _name.as_str().trim_start_matches(OSS_PREFIX).to_owned(),
                        _name.as_str().to_owned(),
                        v.to_str().unwrap_or("Has invisible Ascii chars").to_owned(),
                    );
                } else {
                    headers.insert(
                        _name.as_str().to_owned(),
                        v.to_str().unwrap_or("Has invisible Ascii chars").to_owned(),
                    );
                }
            }
        }
        Self {
            content,
            meta,
            headers,
        }
    }
}

impl From<GetAsBufferResp> for GetResp {
    fn from(resp: GetAsBufferResp) -> Self {
        let meta = resp.meta;
        let headers = resp.headers;
        let content = resp.content;
        Self {
            content: String::from_utf8(content.to_vec())
                .unwrap_or("Failed when encoding to string.".to_owned()),
            meta,
            headers,
        }
    }
}

impl ListDetailsResp {
    pub(crate) fn to_obj_names<R>(self) -> R
    where
        R: FromIterator<String>,
    {
        R::from_iter(self.objects.into_iter().map(|obj| obj.key))
    }
}

pub struct PutOrCopyOptions<'a> {
    pub meta: Vec<(&'a str, &'a str)>,
    pub content_type: &'a str,
    pub cache_control: &'a str,
    pub content_disposition: &'a str,
    pub content_encoding: &'a str,
}

impl<'a> PutOrCopyOptions<'a> {
    pub fn new<S, MM, M>(
        meta: M,
        content_type: S,
        cache_control: S,
        content_disposition: S,
        content_encoding: S,
    ) -> Self
    where
        S: Into<Option<&'a str>>,
        MM: Default + IntoIterator<Item = (&'a str, &'a str)>,
        M: Into<Option<MM>>,
    {
        Self {
            meta: meta.into().unwrap_or_default().into_iter().collect(),
            cache_control: cache_control.into().unwrap_or_default(),
            content_type: content_type.into().unwrap_or_default(),
            content_disposition: content_disposition.into().unwrap_or_default(),
            content_encoding: content_encoding.into().unwrap_or_default(),
        }
    }

    pub(crate) fn to_headers(&self) -> Vec<(String, String)> {
        let mut headers_vec = Vec::with_capacity(4);
        let mut add_headers = |k: &str, v: &str| {
            if !v.is_empty() {
                headers_vec.push((k.to_owned(), v.to_owned()));
            }
        };
        add_headers("cache_control", self.content_type);
        add_headers("content_type", self.content_type);
        add_headers("content_disposition", self.content_type);
        add_headers("content_encoding", self.content_type);
        headers_vec
    }
}

pub struct ListOptions<'a> {
    pub prefix: &'a str,
    pub marker: &'a str,
    pub delimiter: &'a str,
    pub max_keys: usize,
}
impl<'a> ListOptions<'a> {
    pub fn new<S1, S2, S3, N>(prefix: S1, marker: S2, delimiter: S3, max_keys: N) -> Self
    where
        S1: Into<Option<&'a str>>,
        S2: Into<Option<&'a str>>,
        S3: Into<Option<&'a str>>,
        N: Into<Option<usize>>,
    {
        Self {
            prefix: prefix.into().unwrap_or_default(),
            marker: marker.into().unwrap_or_default(),
            delimiter: delimiter.into().unwrap_or_default(),
            max_keys: max_keys.into().unwrap_or(1000),
        }
    }
    pub(crate) fn to_params(&self) -> Vec<(String, Option<String>)> {
        let mut params_vec = Vec::with_capacity(4);
        let mut add_params = |k: &str, v: &str| {
            if !v.is_empty() {
                params_vec.push((k.to_owned(), Some(v.to_owned())));
            }
        };
        add_params("prefix", self.prefix);
        add_params("marker", self.marker);
        add_params("delimiter", self.delimiter);
        params_vec.push(("max_keys".to_owned(), Some(self.max_keys.to_string())));
        params_vec
    }
}

pub struct SignUrlOptions<'a> {
    pub method: &'a str,
    pub expires: u64,
}

impl<'a> SignUrlOptions<'a> {
    pub fn new<M, E>(method: M, expires: E) -> Self
    where
        M: Into<Option<&'a str>>,
        E: Into<Option<u64>>,
    {
        Self {
            method: method.into().unwrap_or("GET"),
            expires: expires.into().unwrap_or(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Time went backwards.")
                    .as_secs()
                    + 3600,
            ),
        }
    }
}

impl<'a> Default for SignUrlOptions<'a> {
    fn default() -> Self {
        Self {
            method: "GET",
            expires: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went back wards")
                .as_secs()
                + 3600,
        }
    }
}
