use std::collections::HashMap;

use super::*;
use crate::{aws::S3Client, inner_client::InnerClient};

pub trait AwosApi {
    /// 获取当前 Bucket 下 Objects 的名称列表。 以 Vector 返回。
    /// 可选参数详见其定义。
    fn list_object<'a, O>(&self, opts: O) -> Result<Vec<String>>
    where
        O: Into<Option<ListOptions<'a>>>;

    /// 获取当前 Bucket 下 Objects 的信息列表
    /// 可选参数详见其定义。
    fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>>;

    /// Get 一个 Object
    /// 可选参数是一个 Metas 的过滤器， 仅在此中指定的 Metas 才会被返回。
    fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>;
    /// Get 一个 Object, Content 为在buffer中的二进制数据。
    /// 可选参数是一个 Metas 的过滤器，传入 None 时不过滤， 传入其他集合类型时，仅返集合中指定的 Metas。
    ///
    /// TODO: 因为这里用了泛型， 传入 None 的时候无法推断出 F 的类型， 只能通过 ::<> 传入一个类型变量。
    ///       不是很好用，需要找个方法改进一下。

    fn get_as_buffer<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>;
    fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str>;
    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        O: Into<Option<PutOrCopyOptions<'a>>>;
    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        O: Into<Option<PutOrCopyOptions<'a>>>;
    fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str>;
    fn del_multi<K, S>(&self, keys: K) -> Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>;

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<SignedUrlOptions<'a>>>;
}

pub struct AwosClient {
    inner: InnerClient,
    // is_internal: bool,
}

impl AwosClient {
    /// AWOS client, with OSS internal.
    /// # Args
    /// enpoint: Public enpoint
    /// bucket: None or Strings alike.
    /// access_key_id: Strings alike. e.g. "JjknmtKqNHJGEXpJmHsfjNm8"
    /// access_key_id: Strings alike. e.g. "5wWr3xm1mGmPBM0wsRz48VTiNEXq6z"
    pub fn new_with_oss<'a, S1, S2, S3, S4>(
        endpoint: S1,
        bucket: S2,
        access_key_id: S3,
        access_key_secret: S4,
    ) -> Result<Self>
    where
        S1: AsRef<str>,
        S2: Into<Option<&'a str>>,
        S3: Into<String>,
        S4: Into<String>,
    {
        let url = endpoint.as_ref();
        let schema = if url.starts_with("https") {
            "https"
        } else {
            "http"
        };
        let region = url.trim_start_matches(schema).trim_start_matches("://");
        Ok(Self {
            inner: InnerClient::OSS(OSSClient::new_oss_cli(
                region,
                schema,
                bucket,
                access_key_id,
                access_key_secret,
            )),
        })
    }

    pub fn new_with_s3<'a, S1, S2, S3, S4>(
        endpoint: S1,
        bucket: S2,
        access_key_id: S3,
        access_key_secret: S4,
    ) -> Result<Self>
    where
        S1: Into<String>,
        S2: Into<Option<String>>,
        S3: Into<String>,
        S4: Into<String>,
    {
        let inner = InnerClient::AWS(S3Client::new_s3_cli(
            endpoint.into(),
            bucket.into().unwrap_or_default(),
            access_key_id.into(),
            access_key_secret.into(),
        )?);
        Ok(Self { inner })
    }
}

impl AwosApi for AwosClient {
    fn list_object<'a, O>(&self, opts: O) -> Result<Vec<String>>
    where
        O: Into<Option<ListOptions<'a>>>,
    {
        self.inner.list_object(opts)
    }

    fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>>,
    {
        self.inner.list_details(opts)
    }

    fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>,
    {
        self.inner.get(key, meta_keys_filter)
    }

    fn get_as_buffer<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>,
    {
        self.inner.get_as_buffer(key, meta_keys_filter)
    }

    fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str>,
    {
        self.inner.head(key)
    }

    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        O: Into<Option<PutOrCopyOptions<'a>>>,
    {
        self.inner.put(key, data, opts)
    }

    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        O: Into<Option<PutOrCopyOptions<'a>>>,
    {
        self.inner.copy(src, key, opts)
    }

    fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str>,
    {
        self.inner.del(key)
    }

    fn del_multi<K, S>(&self, keys: K) -> Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>,
    {
        self.inner.del_multi(keys)
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<SignedUrlOptions<'a>>>,
    {
        self.inner.sign_url(key, opts)
    }
}
