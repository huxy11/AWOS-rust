use super::*;

use oss_sdk::OssClient;

use crate::AwosApi;

pub(crate) struct S3Client {
    pub(crate) inner: S3Inner,
    pub(crate) bucket: String,
    pub(crate) region: String,
}

pub(crate) enum InnerClient {
    AWS(S3Client),
    OSS(OssClient),
}

impl AwosApi for InnerClient {
    fn list_object<'a, O, R>(&self, opts: O) -> crate::errors::Result<R>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
        R: std::iter::FromIterator<String>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.list_object(opts),
        }
    }

    fn list_details<'a, O>(&self, opts: O) -> crate::errors::Result<crate::ListDetailsResp>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.list_details(opts),
        }
    }

    fn get<'a, S, M>(&self, key: S, meta_keys_filter: M) -> crate::errors::Result<crate::GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<std::collections::HashSet<&'a str>>>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.get(key, meta_keys_filter),
        }
    }

    fn get_as_buffer<'a, S, M>(
        &self,
        key: S,
        meta_keys_filter: M,
    ) -> crate::errors::Result<crate::GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<std::collections::HashSet<&'a str>>>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.get_as_buffer(key, meta_keys_filter),
        }
    }

    fn head<S>(&self, key: S) -> crate::errors::Result<std::collections::HashMap<String, String>>
    where
        S: AsRef<str>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.head(key),
        }
    }

    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        // M: Default + IntoIterator<Item = (&'a str, &'a str)>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.put(key, data, opts),
        }
    }

    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> crate::errors::Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        // M: Default + IntoIterator<Item = (&'a str, &'a str)>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.copy(src, key, opts),
        }
    }

    fn del<S>(&self, key: S) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.del(key),
        }
    }

    fn del_multi<K, S>(&self, keys: K) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.del_multi(keys),
        }
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> crate::errors::Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<crate::SignUrlOptions<'a>>>,
    {
        match self {
            InnerClient::AWS(_) => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.sign_url(key, opts),
        }
    }
}
