use std::collections::HashMap;

use crate::{aws::S3Client, errors::Result, types, GetAsBufferResp, PutOrCopyOptions};
use async_trait::async_trait;
use oss_sdk::OssClient;

use crate::{AwosApi, ListDetailsResp, ListOptions};

pub(crate) enum InnerClient {
    AWS(S3Client),
    OSS(OssClient),
}

#[async_trait]
impl AwosApi for InnerClient {
    async fn list_object<'a, O>(&self, opts: O) -> crate::errors::Result<Vec<String>>
    where
        O: Into<Option<crate::ListOptions<'a>>> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.list_object(opts).await,
            InnerClient::OSS(_oss_client) => _oss_client.list_object(opts).await,
            // _ => unimplemented!(),
        }
    }

    async fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.list_details(opts).await,
            InnerClient::OSS(_oss_client) => _oss_client.list_details(opts).await,
            // _ => unimplemented!(),
        }
    }

    async fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<types::GetResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.get(key, meta_keys_filter).await,
            InnerClient::OSS(_oss_client) => _oss_client.get(key, meta_keys_filter).await,
            // _ => unimplemented!(),
        }
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
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.get_as_buffer(key, meta_keys_filter).await,
            InnerClient::OSS(_oss_client) => _oss_client.get_as_buffer(key, meta_keys_filter).await,
            // _ => unimplemented!(),
        }
    }

    async fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.head(key).await,
            InnerClient::OSS(_oss_client) => _oss_client.head(key).await,
            // _ => unimplemented!(),
        }
    }

    async fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str> + Send,
        D: Into<Box<[u8]>> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.put(key, data, opts).await,
            InnerClient::OSS(_oss_client) => _oss_client.put(key, data, opts).await,
            // _ => unimplemented!(),
        }
    }

    async fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String> + Send,
        S2: AsRef<str> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.copy(src, key, opts).await,
            InnerClient::OSS(_oss_client) => _oss_client.copy(src, key, opts).await,
            // _ => unimplemented!(),
        }
    }

    async fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str> + Send,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.del(key).await,
            InnerClient::OSS(_oss_client) => _oss_client.del(key).await,
            // _ => unimplemented!(),
        }
    }

    async fn del_multi<S>(&self, keys: &[S]) -> Result<()>
    where
        S: AsRef<str> + Sync,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.del_multi(keys).await,
            InnerClient::OSS(_oss_client) => _oss_client.del_multi(keys).await,
            // _ => unimplemented!(),
        }
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> crate::errors::Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<crate::SignedUrlOptions<'a>>>,
    {
        match self {
            InnerClient::AWS(_s3_client) => _s3_client.sign_url(key, opts),
            InnerClient::OSS(_oss_client) => _oss_client.sign_url(key, opts),
            // _ => unimplemented!(),
        }
    }
}
