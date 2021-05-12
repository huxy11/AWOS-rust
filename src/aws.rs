use std::collections::HashMap;

use async_trait::async_trait;
use rusoto_core::HttpClient;
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{
    CopyObjectRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest,
    ListObjectsRequest, PutObjectRequest, S3Client as S3Inner, S3,
};
use rusoto_signature::Region;

use crate::{prelude::*, types, GetAsBufferResp, ListDetailsResp, ListOptions, PutOrCopyOptions};

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

#[async_trait]
impl AwosApi for S3Client {
    async fn list_object<'a, O>(&self, opts: O) -> Result<Vec<String>>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
    {
        let resp = self.list_details(opts).await?;
        Ok(resp.to_obj_names())
    }

    async fn list_details<'a, O>(&self, opts: O) -> Result<ListDetailsResp>
    where
        O: Into<Option<ListOptions<'a>>> + Send,
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
        // let rt = tokio::runtime::Runtime::new().unwrap();
        let resp = self.inner.list_objects(rqst).await?;
        Ok(resp.into())
    }

    async fn get<'a, S, M, F>(&self, key: S, meta_keys_filter: M) -> Result<types::GetResp>
    where
        S: AsRef<str> + Send,
        M: Into<Option<F>> + Send,
        F: IntoIterator<Item = &'a str> + Send,
    {
        let resp = self.get_as_buffer(key, meta_keys_filter).await?;
        Ok(resp.into())
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
        let rqst = GetObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        let mut resp = GetAsBufferResp::from_get_output(self.inner.get_object(rqst).await?).await;
        if let Some(_meta_keys_filter) = meta_keys_filter.into() {
            let _filter = _meta_keys_filter.into_iter().collect();
            resp.filter(_filter);
        }
        Ok(resp)
    }

    async fn head<S>(&self, key: S) -> Result<HashMap<String, String>>
    where
        S: AsRef<str> + Send,
    {
        let rqst = HeadObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        let mut _resp = self.inner.head_object(rqst).await?;
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

    async fn put<'a, S, D, O>(&self, key: S, data: D, opts: O) -> Result<()>
    where
        S: AsRef<str> + Send,
        D: Into<Box<[u8]>> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
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
        self.inner.put_object(rqst).await?;
        Ok(())
    }

    async fn copy<'a, S1, S2, O>(&self, src: S1, key: S2, opts: O) -> Result<()>
    where
        S1: Into<String> + Send,
        S2: AsRef<str> + Send,
        O: Into<Option<PutOrCopyOptions<'a>>> + Send,
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

    async fn del<S>(&self, key: S) -> Result<()>
    where
        S: AsRef<str> + Send,
    {
        let del_request = DeleteObjectRequest {
            bucket: self.bucket.to_owned(),
            key: key.as_ref().to_owned(),
            ..Default::default()
        };
        self.inner.delete_object(del_request).await?;
        Ok(())
    }

    async fn del_multi<S>(&self, keys: &[S]) -> Result<()>
    where
        S: AsRef<str> + Sync,
    {
        for key in keys.into_iter() {
            self.del(key).await?;
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
    #[tokio::test]
    async fn s3_client_test() {
        let bucket = "s3-test-bucket".to_owned();
        let endpoint = "http://127.0.0.1:9000".to_owned();
        let access_key_id = "minioadmin".to_owned();
        let access_key_secret = "minioadmin".to_owned();
        let s3_client =
            S3Client::new_s3_cli(endpoint, bucket, access_key_id, access_key_secret).unwrap();
        let ret = s3_client
            .put("s3_test_file", "S3TESTFILECONTENT".as_bytes(), None)
            .await;
        println!("{:?}", ret);
        assert!(ret.is_ok());
        // let resp = s3_client.get("s3-test-file", None);
    }
}

// #[derive(SerializeToMaps)]
// pub struct AWSHeadObjectOutput {
//     /// <p>Indicates that a range of bytes was specified.</p>
//     #[label("headers")]
//     pub accept_ranges: Option<String>,
//     /// <p>The archive state of the head object.</p>
//     #[label("headers")]
//     pub archive_status: Option<String>,
//     /// <p>Indicates whether the object uses an S3 Bucket Key for server-side encryption with AWS KMS (SSE-KMS).</p>
//     #[label("headers")]
//     pub bucket_key_enabled: Option<bool>,
//     /// <p>Specifies caching behavior along the request/reply chain.</p>
//     #[label("headers")]
//     pub cache_control: Option<String>,
//     /// <p>Specifies presentational information for the object.</p>
//     #[label("headers")]
//     pub content_disposition: Option<String>,
//     /// <p>Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.</p>
//     #[label("headers")]
//     pub content_encoding: Option<String>,
//     /// <p>The language the content is in.</p>
//     #[label("headers")]
//     pub content_language: Option<String>,
//     /// <p>Size of the body in bytes.</p>
//     #[label("headers")]
//     pub content_length: Option<i64>,
//     /// <p>A standard MIME type describing the format of the object data.</p>
//     #[label("headers")]
//     pub content_type: Option<String>,
//     /// <p>Specifies whether the object retrieved was (true) or was not (false) a Delete Marker. If false, this response header does not appear in the response.</p>
//     #[label("headers")]
//     pub delete_marker: Option<bool>,
//     /// <p>An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL.</p>
//     #[label("headers")]
//     pub e_tag: Option<String>,
//     /// <p>If the object expiration is configured (see PUT Bucket lifecycle), the response includes this header. It includes the expiry-date and rule-id key-value pairs providing object expiration information. The value of the rule-id is URL encoded.</p>
//     #[label("headers")]
//     pub expiration: Option<String>,
//     /// <p>The date and time at which the object is no longer cacheable.</p>
//     #[label("headers")]
//     pub expires: Option<String>,
//     /// <p>Last modified date of the object</p>
//     #[label("headers")]
//     pub last_modified: Option<String>,
//     /// <p>A map of metadata to store with the object in S3.</p>
//     pub metadata: Option<::std::collections::HashMap<String, String>>,
//     /// <p>This is set to the number of metadata entries not returned in <code>x-amz-meta</code> headers. This can happen if you create metadata using an API like SOAP that supports more flexible metadata than the REST API. For example, using SOAP, you can create metadata whose values are not legal HTTP headers.</p>
//     #[label("headers")]
//     pub missing_meta: Option<i64>,
//     /// <p>Specifies whether a legal hold is in effect for this object. This header is only returned if the requester has the <code>s3:GetObjectLegalHold</code> permission. This header is not returned if the specified version of this object has never had a legal hold applied. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a>.</p>
//     #[label("headers")]
//     pub object_lock_legal_hold_status: Option<String>,
//     /// <p>The Object Lock mode, if any, that's in effect for this object. This header is only returned if the requester has the <code>s3:GetObjectRetention</code> permission. For more information about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object Lock</a>. </p>
//     #[label("headers")]
//     pub object_lock_mode: Option<String>,
//     /// <p>The date and time when the Object Lock retention period expires. This header is only returned if the requester has the <code>s3:GetObjectRetention</code> permission.</p>
//     #[label("headers")]
//     pub object_lock_retain_until_date: Option<String>,
//     /// <p>The count of parts this object has.</p>
//     #[label("headers")]
//     pub parts_count: Option<i64>,
//     /// <p>Amazon S3 can return this header if your request involves a bucket that is either a source or a destination in a replication rule.</p> <p>In replication, you have a source bucket on which you configure replication and destination bucket or buckets where Amazon S3 stores object replicas. When you request an object (<code>GetObject</code>) or object metadata (<code>HeadObject</code>) from these buckets, Amazon S3 will return the <code>x-amz-replication-status</code> header in the response as follows:</p> <ul> <li> <p>If requesting an object from the source bucket — Amazon S3 will return the <code>x-amz-replication-status</code> header if the object in your request is eligible for replication.</p> <p> For example, suppose that in your replication configuration, you specify object prefix <code>TaxDocs</code> requesting Amazon S3 to replicate objects with key prefix <code>TaxDocs</code>. Any objects you upload with this key name prefix, for example <code>TaxDocs/document1.pdf</code>, are eligible for replication. For any object request with this key name prefix, Amazon S3 will return the <code>x-amz-replication-status</code> header with value PENDING, COMPLETED or FAILED indicating object replication status.</p> </li> <li> <p>If requesting an object from a destination bucket — Amazon S3 will return the <code>x-amz-replication-status</code> header with value REPLICA if the object in your request is a replica that Amazon S3 created and there is no replica modification replication in progress.</p> </li> <li> <p>When replicating objects to multiple destination buckets the <code>x-amz-replication-status</code> header acts differently. The header of the source object will only return a value of COMPLETED when replication is successful to all destinations. The header will remain at value PENDING until replication has completed for all destinations. If one or more destinations fails replication the header will return FAILED. </p> </li> </ul> <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Replication</a>.</p>
//     #[label("headers")]
//     pub replication_status: Option<String>,
//     #[label("headers")]
//     pub request_charged: Option<String>,
//     /// <p>If the object is an archived object (an object whose storage class is GLACIER), the response includes this header if either the archive restoration is in progress (see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html">RestoreObject</a> or an archive copy is already restored.</p> <p> If an archive copy is already restored, the header value indicates when Amazon S3 is scheduled to delete the object copy. For example:</p> <p> <code>x-amz-restore: ongoing-request="false", expiry-date="Fri, 23 Dec 2012 00:00:00 GMT"</code> </p> <p>If the object restoration is in progress, the header returns the value <code>ongoing-request="true"</code>.</p> <p>For more information about archiving objects, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html#lifecycle-transition-general-considerations">Transitioning Objects: General Considerations</a>.</p>
//     #[label("headers")]
//     pub restore: Option<String>,
//     /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header confirming the encryption algorithm used.</p>
//     #[label("headers")]
//     pub sse_customer_algorithm: Option<String>,
//     /// <p>If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide round-trip message integrity verification of the customer-provided encryption key.</p>
//     #[label("headers")]
//     pub sse_customer_key_md5: Option<String>,
//     /// <p>If present, specifies the ID of the AWS Key Management Service (AWS KMS) symmetric customer managed customer master key (CMK) that was used for the object.</p>
//     #[label("headers")]
//     pub ssekms_key_id: Option<String>,
//     /// <p>If the object is stored using server-side encryption either with an AWS KMS customer master key (CMK) or an Amazon S3-managed encryption key, the response includes this header with the value of the server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).</p>
//     #[label("headers")]
//     pub server_side_encryption: Option<String>,
//     /// <p>Provides storage class information of the object. Amazon S3 returns this header for all objects except for S3 Standard storage class objects.</p> <p>For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage Classes</a>.</p>
//     #[label("headers")]
//     pub storage_class: Option<String>,
//     /// <p>Version of the object.</p>
//     #[label("headers")]
//     pub version_id: Option<String>,
//     /// <p>If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata.</p>
//     #[label("headers")]
//     pub website_redirect_location: Option<String>,
// }
