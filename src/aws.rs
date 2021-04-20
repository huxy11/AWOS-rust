use std::collections::HashMap;

use super::*;
use rusoto_core::HttpClient;
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{CopyObjectRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest, PutObjectRequest};
use rusoto_s3::{ListObjectsRequest, S3Client as S3Inner, S3};
use rusoto_signature::Region;

use crate::AwosApi;
pub(crate) struct S3Client {
    pub(crate) inner: S3Inner,
    pub(crate) bucket: String,
    pub(crate) region: Region,
    // Used in generate Presigned url
    credentials: AwsCredentials,
}

impl S3Client {
    pub(crate) fn new_s3_cli(
        endpoint: String,
        bucket: String,
        access_key_id: String,
        access_key_secret: String,
    ) -> Result<Self> {
        let credentials_provider = StaticProvider::new(
            access_key_id.to_owned(),
            access_key_secret.to_owned(),
            None,
            None,
        );
        let credentials = AwsCredentials::new(access_key_id, access_key_secret, None, None);
        let request_dispatcher = HttpClient::new().expect("failed to create request dispatcher");
        let region = Region::Custom {
            name: "CN".to_owned(),
            endpoint: endpoint,
        };
        Ok(Self {
            inner: S3Inner::new_with(request_dispatcher, credentials_provider, region.to_owned()),
            bucket,
            region,
            credentials,
        })
    }
}

impl AwosApi for S3Client {
    fn list_object<'a, O>(&self, opts: O) -> crate::errors::Result<Vec<String>>
    where
        O: Into<Option<crate::ListOptions<'a>>>,
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
        let resp = rt.block_on(self.inner.list_objects(rqst)).map_err(|e| {
            Error::AWS(S3Error::GetError {
                msg: format!("{}", e),
            })
        })?;
        Ok(resp.into())
    }

    fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> crate::errors::Result<crate::GetResp>
    where
        S: AsRef<str>,
        M: Into<Option<F>>,
        F: IntoIterator<Item = &'a str>,
    {
        let resp = self.get_as_buffer(key, meta_keys_filter)?;
        Ok(resp.into())
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
        let rqst = GetObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut resp: GetAsBufferResp = rt
            .block_on(self.inner.get_object(rqst))
            .map_err(|e| {
                Error::AWS(S3Error::GetError {
                    msg: format!("{}", e),
                })
            })?
            .into();
        if let Some(_meta_keys_filter) = meta_keys_filter.into() {
            let _filter = _meta_keys_filter.into_iter().collect();
            resp.filter(_filter);
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
        let _resp = rt.block_on(self.inner.head_object(rqst)).map_err(|e| {
            Error::AWS(S3Error::HeadError {
                msg: format!("{}", e),
            })
        })?;
        let mut ret = HashMap::new();
        let mut _add_headers = |key: &str, val| {
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
        let mut put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            body: Some(buf.into()),
            ..Default::default()
        };
        if let Some(_opts) = opts.into() {
            if !_opts.meta.is_empty() {
                let mut hm = HashMap::new();
                hm.extend(
                    _opts
                        .meta
                        .into_iter()
                        .map(|(k, v)| (k.to_owned(), v.to_owned())),
                );
                put_request.metadata = Some(hm);
            }
            if !_opts.content_type.is_empty() {
                put_request.content_type = Some(_opts.content_type.to_owned());
            }
            if !_opts.cache_control.is_empty() {
                put_request.cache_control = Some(_opts.cache_control.to_owned());
            }
            if !_opts.content_disposition.is_empty() {
                put_request.content_disposition = Some(_opts.content_disposition.to_owned());
            }
            if !_opts.content_encoding.is_empty() {
                put_request.content_encoding = Some(_opts.content_encoding.to_owned());
            }
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.inner.put_object(put_request))
            .map_err(|e| {
                Error::AWS(S3Error::PutError {
                    msg: format!("{}", e),
                })
            })?;
        Ok(())
    }

    fn copy<'a, S1, S2, O>(&self, _src: S1, _key: S2, opts: O) -> crate::errors::Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        let mut copy_request = CopyObjectRequest {
            ..Default::default()
        };
        if let Some(_opts) = opts.into() {
            if !_opts.meta.is_empty() {
                let mut hm = HashMap::new();
                hm.extend(_opts.meta.into_iter());
            }
            if !_opts.content_type.is_empty() {
                copy_request.content_type = Some(_opts.content_type.to_owned());
            }
        }
        Ok(())
    }

    fn del<S>(&self, key: S) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
    {
        let del_request = DeleteObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.inner.delete_object(del_request))
            .map_err(|e| {
                Error::AWS(S3Error::DeleteError {
                    msg: format!("{}", e),
                })
            })?;
        Ok(())
    }

    fn del_multi<K, S>(&self, keys: K) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        K: Default + IntoIterator<Item = S>,
    {
        for key in keys.into_iter() {
            self.del(key)?;
        }
        Ok(())
    }

    fn sign_url<'a, S, O>(&self, key: S, opts: O) -> crate::errors::Result<String>
    where
        S: AsRef<str>,
        O: Into<Option<crate::SignUrlOptions<'a>>>,
    {
        let request_uri = format!("/{}/{}", self.bucket, key.as_ref());
        let mut method = "GET";
        let mut expires = 3600;
        if let Some(_opts) = opts.into() {
            if !_opts.method.is_empty() {
                method = _opts.method;
            }
            if _opts.expires != 0 {
                expires = _opts.expires;
            }
        }
        let mut sign_rqst =
            rusoto_signature::SignedRequest::new(method, "s3", &self.region, &request_uri);
        let ret = sign_rqst.generate_presigned_url(
            &self.credentials,
            &std::time::Duration::from_secs(expires),
            false,
        );
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn s3_client_test() {
        let bucket = "s3-test-bucket".to_owned();
        let endpoint = "http://127.0.0.1:9000".to_owned();
        let access_key_id = "minioadmin".to_owned();
        let access_key_secret = "minioadmin".to_owned();
        let s3_client =
            S3Client::new_s3_cli(endpoint, bucket, access_key_id, access_key_secret).unwrap();
        let ret = s3_client.put("s3_test_file", "S3TESTFILECONTENT".as_bytes(), None);
        println!("{:?}", ret);
        assert!(ret.is_ok());
        // let resp = s3_client.get("s3-test-file", None);
    }
}
