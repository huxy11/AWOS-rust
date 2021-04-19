use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
};

use crate::inner_client::{InnerClient, S3Client};
use rusoto_core::HttpClient;
use rusoto_credential::StaticProvider;
use rusoto_s3::S3Client as S3Inner;

use super::*;

pub trait AwosApi {
    fn list_object<'a, O, R>(&self, opts: O) -> Result<R>
    where
        O: Into<Option<ListOptions<'a>>>,
        R: FromIterator<String>;
    fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>>;
    fn get<'a, S, M>(&self, key: S, meta_keys_filter: M) -> Result<GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<HashSet<&'a str>>>;
    fn get_as_buffer<'a, S, M>(&self, key: S, meta_keys_filter: M) -> Result<GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<HashSet<&'a str>>>;
    fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str>;
    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        // M: Default + IntoIterator<Item = (&'a str, &'a str)>,
        O: Into<Option<PutOrCopyOptions<'a>>>;
    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        // M: Default + IntoIterator<Item = (&'a str, &'a str)>,
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
        O: Into<Option<SignUrlOptions<'a>>>;
}

pub struct AwosClient {
    inner: InnerClient,
}

impl AwosClient {
    /// AWOS client, with OSS internal.
    /// # Args
    /// region: Strings alike. Invalid input would be treated as default. e.g. "北京", "oss-cn-beijing".to_string()
    /// shcema: None or Strings alike. Invalid input and None would be treated as default. e.g. None, "http", "HTTPS".to_string()
    /// bucket: None or Strings alike.
    /// access_key_id: Strings alike. e.g. "JjknmtKqNHJGEXpJmHsfjNm8"
    /// access_key_id: Strings alike. e.g. "5wWr3xm1mGmPBM0wsRz48VTiNEXq6z"
    pub fn new_with_oss<'a, S1, S2, S3, S4, S5>(
        region: S1,
        schema: S2,
        bucket: S3,
        access_key_id: S4,
        access_key_secret: S5,
    ) -> Result<Self>
    where
        S1: AsRef<str>,
        S2: Into<Option<&'a str>>,
        S3: Into<Option<&'a str>>,
        S4: Into<String>,
        S5: Into<String>,
    {
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
    pub fn new_with_aws<'a, S1, S2, S3, S4, S5>(
        region: S1,
        schema: S2,
        bucket: S3,
        access_key_id: S4,
        access_key_secret: S5,
    ) -> Result<Self>
    where
        S1: AsRef<str>,
        S2: Into<Option<&'a str>>,
        S3: Into<Option<&'a str>>,
        S4: Into<String>,
        S5: Into<String>,
    {
        let credentials_provider =
            StaticProvider::new(access_key_id.into(), access_key_secret.into(), None, None);
        let request_dispatcher = HttpClient::new().expect("failed to create request dispatcher");
        // let aws_client =
        let s3_client = S3Client {
            inner: S3Inner::new_with(
                request_dispatcher,
                credentials_provider,
                region.as_ref().parse()?,
            ),
            bucket: bucket.into().unwrap_or_default().to_string(),
            region: region.as_ref().to_string(),
        };
        Ok(Self {
            inner: InnerClient::AWS(s3_client),
        })
    }
}

impl AwosApi for AwosClient {
    fn list_object<'a, O, R>(&self, opts: O) -> Result<R>
    where
        O: Into<Option<ListOptions<'a>>>,
        R: FromIterator<String>,
    {
        self.inner.list_object(opts)
    }

    fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>>,
    {
        self.inner.list_details(opts)
    }

    fn get<'a, S, M>(&self, key: S, meta_keys_filter: M) -> Result<GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<HashSet<&'a str>>>,
    {
        self.inner.get(key, meta_keys_filter)
    }

    fn get_as_buffer<'a, S, M>(&self, key: S, meta_keys_filter: M) -> Result<GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<HashSet<&'a str>>>,
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
        O: Into<Option<SignUrlOptions<'a>>>,
    {
        self.inner.sign_url(key, opts)
    }
}
