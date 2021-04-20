use oss_sdk::OssClient;

use crate::AwosApi;

pub(crate) enum InnerClient {
    AWS,
    OSS(OssClient),
}

impl Default for InnerClient {
    fn default() -> Self {
        Self::AWS
    }
}

impl AwosApi for InnerClient {
    fn list_object<'a, O>(&self, opts: O) -> crate::errors::Result<Vec<String>>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.list_object(opts),
        }
    }

    fn list_details<'a, O>(&self, opts: O) -> crate::errors::Result<crate::ListDetailsResp>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.list_details(opts),
        }
    }

    fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> crate::errors::Result<crate::GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.get(key, meta_keys_filter),
        }
    }

    fn get_as_buffer<'a, S, M, F>(
        &self,
        key: S,
        meta_keys_filter: M,
    ) -> crate::errors::Result<crate::GetAsBufferResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.get_as_buffer(key, meta_keys_filter),
        }
    }

    fn head<S>(&self, key: S) -> crate::errors::Result<std::collections::HashMap<String, String>>
    where
        S: AsRef<str>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.head(key),
        }
    }

    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.put(key, data, opts),
        }
    }

    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> crate::errors::Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.copy(src, key, opts),
        }
    }

    fn del<S>(&self, key: S) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.del(key),
        }
    }

    fn del_multi<K, S>(&self, keys: K) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.del_multi(keys),
        }
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> crate::errors::Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<crate::SignUrlOptions<'a>>>,
    {
        match self {
            InnerClient::AWS => unimplemented!(),
            InnerClient::OSS(_oss_client) => _oss_client.sign_url(key, opts),
        }
    }
}
