use bytes::Bytes;
use rusoto_s3::{GetObjectOutput, ListObjectsOutput};
use std::{
    collections::{HashMap, HashSet},
    io::Read,
    iter::FromIterator,
    pin::Pin,
    time::SystemTime,
};

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
/// is_truncated:   是否被截断。
/// objects:        结果列表。
/// prefix:         返回的 Prefix Header 项
/// next_marker:    返回的 Next_marker Header 项，用于连续请求。
#[derive(Clone, Debug, Default)]
pub struct ListDetailsResp {
    pub is_truncated: bool,
    pub objects: Vec<ObjectDetails>,
    pub prefix: String,
    pub next_marker: String,
}
/// Object 的详细信息
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
impl From<GetObjectOutput> for GetAsBufferResp {
    fn from(mut resp: GetObjectOutput) -> Self {
        let mut buf = Vec::new();
        let meta = resp.metadata.take().unwrap_or_default();
        let mut headers = HashMap::new();
        if let Some(_cache_control) = resp.cache_control.take() {
            headers.insert("cache-control".to_owned(), _cache_control);
        }
        if let Some(_body) = resp.body.take() {
            //TODO Async
            _body.into_blocking_read().read_to_end(&mut buf).ok();
        }
        let content = Box::pin(buf.into());
        Self {
            content,
            meta,
            headers,
        }
    }
}

impl GetAsBufferResp {
    pub(crate) fn filter(&mut self, meta_keys_filter: HashSet<&str>) {
        self.meta = std::mem::take(&mut self.meta)
            .into_iter()
            .filter(|(k, _)| {
                meta_keys_filter.contains(k.as_str().trim_start_matches("x-oss-meta-"))
                    || meta_keys_filter.contains(k.as_str())
            })
            .collect();
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
impl From<ListObjectsOutput> for ListDetailsResp {
    fn from(mut out_put: ListObjectsOutput) -> Self {
        let objects = out_put.contents.take().map(|_obj_vec| {
            _obj_vec
                .into_iter()
                .map(|mut _obj| ObjectDetails {
                    key: _obj.key.take().unwrap_or_default(),
                    last_modified: _obj.last_modified.take().unwrap_or_default(),
                    e_tag: _obj.e_tag.take().unwrap_or_default(),
                    size: _obj
                        .size
                        .take()
                        .map(|_size| _size.to_string())
                        .unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        });
        ListDetailsResp {
            is_truncated: out_put.is_truncated.take().unwrap_or_default(),
            next_marker: out_put.next_marker.take().unwrap_or_default(),
            prefix: out_put.prefix.take().unwrap_or_default(),
            objects: objects.unwrap_or_default(),
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

/// 上传/复制 Object 方法的可选参数
/// 不为空时，会在请求中添加对映的 Header
pub struct PutOrCopyOptions<'a> {
    pub meta: Vec<(&'a str, &'a str)>,
    pub content_type: &'a str,
    pub cache_control: &'a str,
    pub content_disposition: &'a str,
    pub content_encoding: &'a str,
}

impl<'a> PutOrCopyOptions<'a> {
    /// 上传/复制方法可选参参数的构建, 其生命周期与传入 String Literal 中最短者一致。
    /// meta, 键置对的集合，
    ///
    /// #Example
    /// ```
    /// let put_opts = awos_rust::PutOrCopyOptions::new(vec![("test-key", "test-val")], "content-type-unknown", None, None, None);
    /// ```
    pub fn new<M, KV, S1, S2, S3, S4>(
        meta: M,
        content_type: S1,
        cache_control: S2,
        content_disposition: S3,
        content_encoding: S4,
    ) -> Self
    where
        M: Into<Option<KV>>,
        KV: Default + IntoIterator<Item = (&'a str, &'a str)>,
        S1: Into<Option<&'a str>>,
        S2: Into<Option<&'a str>>,
        S3: Into<Option<&'a str>>,
        S4: Into<Option<&'a str>>,
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
/// List 相关操作的可选参数
/// 生命周期与传入的 String Literal References 中最短的一致
pub struct ListOptions<'a> {
    pub prefix: &'a str,
    pub marker: &'a str,
    pub delimiter: &'a str,
    pub max_keys: usize,
}
impl<'a> ListOptions<'a> {
    /// ListOptions 构造
    /// 四个参数皆为可选, 传入 None 或者对映类型， 不需要用Some包裹。
    ///
    /// #Example
    /// ```
    /// let list_opts = awos_rust::ListOptions::new("prefix", "marker", None, 2021);
    /// ```

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

/// 构建 Signed Url 需要的参数
pub struct SignUrlOptions<'a> {
    pub method: &'a str,
    pub expires: u64,
}

impl<'a> SignUrlOptions<'a> {
    /// SignUrlOptions 构建
    /// 两个参数均为可选，传入 None 会使用默认值。
    /// method: 默认为 "GET"
    /// expire: 默认为当前时间 + 3600s
    ///
    /// #Example
    /// ```
    /// let sign_url_opts = awos_rust::SignUrlOptions::new("Put", None);
    ///
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
