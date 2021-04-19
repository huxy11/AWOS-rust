use std::collections::HashMap;

use super::*;
use rusoto_core::HttpClient;
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, PutObjectRequest};
use rusoto_s3::{ListObjectsRequest, S3};
use rusoto_signature::Region;

use crate::AwosApi;
pub(crate) struct S3Client {
    pub(crate) inner: S3Inner,
    pub(crate) bucket: String,
    pub(crate) region: Region,
}

impl S3Client {
    pub(crate) fn new(
        bucket: String,
        region: Region,
        access_key_id: String,
        access_key_secret: String,
    ) -> Result<Self> {
        let credentials_provider =
            StaticProvider::new(access_key_id, access_key_secret, None, None);
        let request_dispatcher = HttpClient::new().expect("failed to create request dispatcher");
        Ok(Self {
            inner: S3Inner::new_with(request_dispatcher, credentials_provider, region.to_owned()),
            bucket,
            region,
        })
    }
}

impl AwosApi for S3Client {
    fn list_object<'a, O, R>(&self, opts: O) -> crate::errors::Result<R>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
        R: std::iter::FromIterator<String>,
    {
        let resp = self.list_details(opts)?;
        Ok(resp.to_obj_names())
    }

    fn list_details<'a, O>(&self, opts: O) -> crate::errors::Result<crate::ListDetailsResp>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
    {
        let mut rqst = ListObjectsRequest {
            bucket: self.bucket.to_owned(),
            ..Default::default()
        };
        if let Some(_opts) = opts.into() {
            rqst.max_keys = Some(_opts.max_keys as i64);
            if !_opts.prefix.is_empty() {
                rqst.prefix = Some(_opts.prefix.to_owned());
            }
            if !_opts.marker.is_empty() {
                rqst.marker = Some(_opts.marker.to_owned());
            }
            if !_opts.delimiter.is_empty() {
                rqst.delimiter = Some(_opts.delimiter.to_owned());
            }
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let resp = rt.block_on(self.inner.list_objects(rqst)).map_err(|_| {
            Error::Object(ObjectError::GetError {
                msg: "Unknown".to_owned(),
            })
        })?;
        Ok(resp.into())
    }

    fn get<'a, S, M>(&self, key: S, meta_keys_filter: M) -> crate::errors::Result<crate::GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<std::collections::HashSet<&'a str>>>,
    {
        let resp = self.get_as_buffer(key, meta_keys_filter)?;
        Ok(resp.into())
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
        let rqst = GetObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut resp: GetAsBufferResp = rt
            .block_on(self.inner.get_object(rqst))
            .map_err(|_| {
                Error::Object(ObjectError::GetError {
                    msg: "Unknown".to_owned(),
                })
            })?
            .into();
        if let Some(_meta_keys_filter) = meta_keys_filter.into() {
            resp.filter(_meta_keys_filter);
        }
        Ok(resp)
    }

    fn head<S>(&self, key: S) -> crate::errors::Result<std::collections::HashMap<String, String>>
    where
        S: AsRef<str>,
    {
        let rqst = HeadObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut resp = rt.block_on(self.inner.head_object(rqst)).map_err(|_| {
            Error::Object(ObjectError::HeadError {
                msg: "Unknown".to_owned(),
            })
        })?;
        let mut ret = HashMap::new();
        let mut add_headers = |key: &str, val| {
            if let Some(_val) = val {
                ret.insert(key.to_owned(), _val);
            }
        };
        Ok(ret)
    }

    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        let buf = data.into().to_vec();
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            body: Some(buf.into()),
            ..Default::default()
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.inner.put_object(put_request))
            .map_err(|_| {
                Error::Object(ObjectError::PutError {
                    msg: "Unknown".to_owned(),
                })
            })?;
        Ok(())
    }

    fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> crate::errors::Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        // M: Default + IntoIterator<Item = (&'a str, &'a str)>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        todo!()
    }

    fn del<S>(&self, key: S) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
    {
        todo!()
    }

    fn del_multi<K, S>(&self, keys: K) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>,
    {
        todo!()
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> crate::errors::Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<crate::SignUrlOptions<'a>>>,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn s3_client_test() {
        let bucket = "s3-test-bucket".to_owned();
        let region = Region::Custom {
            endpoint: "http://127.0.0.1:9000".to_owned(),
            name: "LOCAL".to_owned(),
        };
        let access_key_id = "minioadmin".to_owned();
        let access_key_secret = "minioadmin".to_owned();
        let s3_client = S3Client::new(bucket, region, access_key_id, access_key_secret).unwrap();
        let resp = s3_client.get("s3-test-file", None);
    }
}
