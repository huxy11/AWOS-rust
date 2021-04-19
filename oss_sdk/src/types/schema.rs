// #[derive(Clone, Debug, Default, PartialEq)]
// pub struct GetObjRequest {
//     pub bucket: String,
//     /// <p>指定OSS返回请求的content_type头。</p>
//     pub response_content_type: Option<String>,
//     /// <p>指定OSS返回请求的content-language头。</p>
//     pub response_content_language: Option<String>,
//     /// <p>指定OSS返回请求的expires头。</p>
//     pub response_expires: Option<String>,
//     /// <p>指定OSS返回请求的cache-control头。</p>
//     pub response_cache_control: Option<String>,
//     /// <p>指定OSS返回请求的content-disposition头。</p>
//     pub response_content_disposition: Option<String>,
//     /// <p>指定OSS返回请求的content-encoding头。</p>
//     pub response_content_encoding: Option<String>,
//     /// <p>指定文件传输的范围。</p><p><ul><li>如果指定的范围符合规范，返回消息中会包含整个Object的大小和此次返回Object的范围。例如：Content-Range: bytes 0~9/44，表示整个Object大小为44，此次返回的范围为0~9。</li><li>如果指定的范围不符合规范，则传送整个Object，并且结果中不包含Content-Range。</li></ul></p>
//     pub range: Option<String>,
//     /// <p>如果指定的时间早于实际修改时间或指定的时间不符合规范，则直接返回Object，并返回200 OK；如果指定的时间等于或者晚于实际修改时间，则返回304 Not Modified。</p><p>时间格式：GMT，例如Fri, 13 Nov 2015 14:47:53 GMT</p>
//     pub if_modified_since: Option<String>,
//     /// <p>如果指定的时间等于或者晚于Object实际修改时间，则正常传输Object，并返回200 OK；如果指定的时间早于实际修改时间，则返回412 Precondition Failed。</p><p>时间格式：GMT，例如Fri, 13 Nov 2015 14:47:53 GMT</p><p>If-Modified-Since和If-Unmodified-Since可以同时使用。</p>
//     pub if_unmodified_since: Option<String>,
//     /// <p>如果传入的ETag和Object的ETag匹配，则正常传输Object，并返回200 OK；如果传入的ETag和Object的ETag不匹配，则返回412 Precondition Failed。</p>
//     pub if_match: Option<String>,
//     /// <p>如果传入的ETag值和Object的ETag不匹配，则正常传输Object，并返回200 OK；如果传入的ETag和Object的ETag匹配，则返回304 Not Modified。</p><p>If-Match和If-None-Match可以同时使用。</p>
//     pub if_none_match: Option<String>,
//     /// <p>指定客户端的编码类型。</p><p>如果要对返回内容进行Gzip压缩传输，您需要在请求头中以显示方式加入Accept-Encoding:gzip。OSS会根据Object的Content-Type和Object大小（不小于1 KB）判断是否返回经过Gzip压缩的数据。</p><p><strong>说明</strong></p><p><ul><li>如果采用了Gzip压缩，则不会附带ETag信息。</li><li>目前OSS支持Gzip压缩的Content-Type为text/cache-manifest、 text/xml、text/plain、text/css、application/javascript、application/x-javascript、application/rss+xml、application/json和text/json。</li></ul></p>
//     pub accept_encoding: Option<String>,
// }

use std::{
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    str::FromStr,
};

#[derive(Clone, Copy, Debug, Display)]
pub enum Schema {
    #[display(fmt = "http")]
    Http,
    #[display(fmt = "https")]
    Https,
}

impl Default for Schema {
    fn default() -> Self {
        Schema::Https
    }
}
impl FromStr for Schema {
    type Err = ParseSchemaError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "http" => Ok(Schema::Http),
            "https" => Ok(Schema::Https),
            _ => Err(ParseSchemaError::new(s)),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ParseSchemaError {
    message: String,
}
impl ParseSchemaError {
    /// Parses Schema given as a string literal
    pub fn new(input: &str) -> Self {
        ParseSchemaError {
            message: format!("Invalid OSS Schema: {}, ", input),
        }
    }
}

impl StdError for ParseSchemaError {}
impl Display for ParseSchemaError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{}", self.message)
    }
}
