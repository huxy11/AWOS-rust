use crate::{aws::S3Client, inner_client::InnerClient};

use super::*;
// use crate::{aws::S3Client, inner_client::InnerClient};
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
pub trait AwosApi {
    /// 获取当前 Bucket 下 Objects 的名称列表。 以 Vector 返回。
    /// 可选参数详见其定义。
    async fn list_object<'a, O>(&self, opts: O) -> Result<Vec<String>>
    where
        O: Into<Option<ListOptions<'a>>> + Send;

    /// 获取当前 Bucket 下 Objects 的信息列表
    /// 可选参数详见其定义。
    async fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>> + Send;

    /// Get 一个 Object
    /// 可选参数是一个 Metas 的过滤器， 仅在此中指定的 Metas 才会被返回。
    async fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<types::GetResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send;
    /// Get 一个 Object, Content 为在buffer中的二进制数据。
    /// 可选参数是一个 Metas 的过滤器，传入 None 时不过滤， 传入其他集合类型时，仅返集合中指定的 Metas。
    ///
    /// TODO: 因为这里用了泛型， 传入 None 的时候无法推断出 F 的类型， 只能通过 ::<> 传入一个类型变量。
    ///       不是很好用，需要找个方法改进一下。

    async fn get_as_buffer<'a, S, M, F>(
        &self,
        key: S,
        meta_keys_filter: M,
    ) -> Result<GetAsBufferResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send;
    async fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str> + Send;
    async fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str> + Send,
        D: Into<Box<[u8]>> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send;
    async fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String> + Send,
        S2: AsRef<str> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send;
    async fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str> + Send;
    async fn del_multi<S>(&self, keys: &[S]) -> Result<()>
    where
        S: AsRef<str> + Sync;

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

#[async_trait]
impl AwosApi for AwosClient {
    async fn list_object<'a, O>(&self, opts: O) -> Result<Vec<String>>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
    {
        self.inner.list_object(opts).await
    }

    async fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
    {
        self.inner.list_details(opts).await
    }

    async fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<GetResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send,
    {
        self.inner.get(key, meta_keys_filter).await
    }

    async fn get_as_buffer<'a, S, M, F>(
        &self,
        key: S,
        meta_keys_filter: M,
    ) -> Result<GetAsBufferResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send,
    {
        self.inner.get_as_buffer(key, meta_keys_filter).await
    }

    async fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str> + Send,
    {
        self.inner.head(key).await
    }

    async fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str> + Send,
        D: Into<Box<[u8]>> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
    {
        self.inner.put(key, data, opts).await
    }

    async fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String> + Send,
        S2: AsRef<str> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
    {
        self.inner.copy(src, key, opts).await
    }

    async fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str> + Send,
    {
        self.inner.del(key).await
    }

    async fn del_multi<S>(&self, keys: &[S]) -> Result<()>
    where
        S: AsRef<str> + Sync,
    {
        self.inner.del_multi(keys).await
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<SignedUrlOptions<'a>>>,
    {
        self.inner.sign_url(key, opts)
    }
}
