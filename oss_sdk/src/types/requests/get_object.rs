#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct GetObjectRequest {
    /// <p>The bucket name containing the object. </p> <p>When using this API with an access point, you must direct requests to the access point hostname. The access point hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. When using this operation with an access point through the AWS SDKs, you provide the access point ARN in place of the bucket name. For more information about access point ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using Access Points</a> in the <i>Amazon Simple Storage Service Developer Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. When using this operation using S3 on Outposts through the AWS SDKs, you provide the Outposts bucket ARN in place of the bucket name. For more information about S3 on Outposts ARNs, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer Guide</i>.</p>
    pub bucket: String,
    /// <p>The account id of the expected bucket owner. If the bucket is owned by a different account, the request will fail with an HTTP <code>403 (Access Denied)</code> error.</p>
    pub expected_bucket_owner: Option<String>,
    /// <p>Return the object only if its entity tag (ETag) is the same as the one specified, otherwise return a 412 (precondition failed).</p>
    pub if_match: Option<String>,
    /// <p>Return the object only if it has been modified since the specified time, otherwise return a 304 (not modified).</p>
    pub if_modified_since: Option<String>,
    /// <p>Return the object only if its entity tag (ETag) is different from the one specified, otherwise return a 304 (not modified).</p>
    pub if_none_match: Option<String>,
    /// <p>Return the object only if it has not been modified since the specified time, otherwise return a 412 (precondition failed).</p>
    pub if_unmodified_since: Option<String>,
    /// <p>Key of the object to get.</p>
    pub key: String,
    /// <p>Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object.</p>
    pub part_number: Option<i64>,
    /// <p><p>Downloads the specified range bytes of an object. For more information about the HTTP Range header, see <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p> <note> <p>Amazon S3 doesn&#39;t support retrieving multiple ranges of data per <code>GET</code> request.</p> </note></p>
    pub range: Option<String>,
    pub request_payer: Option<String>,
    /// <p>Sets the <code>Cache-Control</code> header of the response.</p>
    pub response_cache_control: Option<String>,
    /// <p>Sets the <code>Content-Disposition</code> header of the response</p>
    pub response_content_disposition: Option<String>,
    /// <p>Sets the <code>Content-Encoding</code> header of the response.</p>
    pub response_content_encoding: Option<String>,
    /// <p>Sets the <code>Content-Language</code> header of the response.</p>
    pub response_content_language: Option<String>,
    /// <p>Sets the <code>Content-Type</code> header of the response.</p>
    pub response_content_type: Option<String>,
    /// <p>Sets the <code>Expires</code> header of the response.</p>
    pub response_expires: Option<String>,
    /// <p>Specifies the algorithm to use to when encrypting the object (for example, AES256).</p>
    pub sse_customer_algorithm: Option<String>,
    /// <p>Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be appropriate for use with the algorithm specified in the <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
    pub sse_customer_key: Option<String>,
    /// <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.</p>
    pub sse_customer_key_md5: Option<String>,
    /// <p>VersionId used to reference a specific version of the object.</p>
    pub version_id: Option<String>,
}