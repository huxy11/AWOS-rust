use std::collections::HashMap;

use rusoto_core::HttpClient;
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{
    CopyObjectRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest, PutObjectRequest,
};
use rusoto_s3::{ListObjectsRequest, S3Client as S3Inner, S3};
use rusoto_signature::Region;

use crate::{prelude::*, GetAsBufferResp};

use crate::AwosApi;
pub(crate) struct S3Client {
    pub(crate) inner: S3Inner,
    pub(crate) bucket: String,
    pub(crate) region: Region,
    // Used in generate Presigned url
    credentials: AwsCredentials,
}

macro_rules! take_and_to_owned {
    ($rqst:ident, $opts:ident, $item:ident) => {
        $rqst.$item = $opts.$item.take().map(|item| item.to_owned());
    };
}
macro_rules! take_headers {
    ($headers:ident, $resp:ident, $($item:ident),+) => {
        $(
            if let Some(_item) = $resp.$item.take() {
                $headers.insert(stringify!($item).replace("_", "_"), _item.to_string());
            }
        )+
    };
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
        if let Some(mut _opts) = opts.into() {
            rqst.max_keys = _opts.max_keys.take().map(|i| i as i64);
            take_and_to_owned!(rqst, _opts, prefix);
            take_and_to_owned!(rqst, _opts, marker);
            take_and_to_owned!(rqst, _opts, delimiter);
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let resp = rt.block_on(self.inner.list_objects(rqst))?;
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
        let mut resp: GetAsBufferResp = rt.block_on(self.inner.get_object(rqst))?.into();
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
        let mut _resp = rt.block_on(self.inner.head_object(rqst))?;
        let mut ret = HashMap::new();
        take_headers!(
            ret,
            _resp,
            accept_ranges,
            cache_control,
            bucket_key_enabled,
            content_length,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            delete_marker,
            e_tag,
            expiration,
            expires,
            last_modified,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            replication_status,
            request_charged,
            restore,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            server_side_encryption,
            storage_class
        );
        Ok(ret)
    }

    fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> crate::errors::Result<()>
    where
        S: AsRef<str>,
        D: Into<Box<[u8]>>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        let buf = data.into().to_vec();
        let mut rqst = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            body: Some(buf.into()),
            ..Default::default()
        };
        if let Some(mut _opts) = opts.into() {
            rqst.metadata = _opts.meta.take();
            take_and_to_owned!(rqst, _opts, content_type);
            take_and_to_owned!(rqst, _opts, cache_control);
            take_and_to_owned!(rqst, _opts, content_disposition);
            take_and_to_owned!(rqst, _opts, content_encoding);
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.inner.put_object(rqst))?;
        Ok(())
    }

    fn copy<'a, S1, S2, O>(&self, _src: S1, _key: S2, opts: O) -> crate::errors::Result<()>
    where
        S1: Into<String>,
        S2: AsRef<str>,
        O: Into<Option<crate::PutOrCopyOptions<'a>>>,
    {
        let mut rqst = CopyObjectRequest {
            ..Default::default()
        };
        if let Some(mut _opts) = opts.into() {
            rqst.metadata = _opts.meta.take();
            take_and_to_owned!(rqst, _opts, content_type);
            take_and_to_owned!(rqst, _opts, content_encoding);
            take_and_to_owned!(rqst, _opts, content_disposition);
            take_and_to_owned!(rqst, _opts, cache_control);
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
        rt.block_on(self.inner.delete_object(del_request))?;
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
        O: Into<Option<crate::SignedUrlOptions<'a>>>,
    {
        let request_uri = format!("/{}/{}", self.bucket, key.as_ref());

        let (method, expires) = if let Some(_opts) = opts.into() {
            (_opts.method.unwrap_or("GET"), _opts.expires.unwrap_or(3600))
        } else {
            ("GET", 3600)
        };
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
