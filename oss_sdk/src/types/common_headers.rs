

#[derive(Debug, Default)]
pub struct CommonRespHeaders {
    /// The content length of an HTTP request defined in <a href="https://www.ietf.org/rfc/rfc2616.txt">RFC 2616</a>
    pub content_length: Option<String>,
    /// The connection status between the client and the OSS server. "Open", "Close" or null.
    pub connection: Option<String>,
    /// The time in GMT stipulated in HTTP/ 1.1. Example: Wed, 05 Sep. 2012 23:00:00 GMT.
    pub date :Option<String>,
    /// The ETag that is created to identify the content of the object when the object is created. If an object is created by using the PutObject request, the ETag value of the object is the MD5 hash of the object content. If an object is created by using another method, the ETag value of the content is the universally unique identifier (UUID) of the object content. The ETag value of the object can be used to check whether the object content is modified.
    pub etag :Option<String>,
    /// The server that generates a response.
    pub server :Option<String>,
    /// The UUID that is created by the OSS server to identify the response. If you encounter any problems when you use OSS, you can contact OSS support personnel and provide this field to help them locate the problem.
    pub x_oss_request_id: Option<String>,
}

