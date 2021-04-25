use awos_rust::{AwosApi, AwosClient, PutOrCopyOptions, SignedUrlOptions};

use std::collections::{HashMap, HashSet};

// use std::error::Error as StdError;

const FILE_NAME: &str = "rust_oss_sdk_test";

const BUF: &[u8] = "This is just a put test".as_bytes();

#[test]
fn awos_with_oss_test() {
    let bucket = std::env::var("OSS_BUCKET").unwrap();
    let access_key_id = std::env::var("OSS_KEY_ID").unwrap();
    let access_key_secret = std::env::var("OSS_KEY_SECRET").unwrap();

    let awos_instance = AwosClient::new_with_oss(
        "https://oss-cn-beijing.aliyuncs.com",
        bucket.as_ref(),
        access_key_id,
        access_key_secret,
    )
    .unwrap();

    /* Put Object */
    let buf = BUF.to_owned().into_boxed_slice();
    let mut meta = HashMap::new();
    meta.insert("test-key".to_owned(), "test-val".to_owned());
    let opts = PutOrCopyOptions::new(meta, None, None, None, None);
    let resp = awos_instance.put(FILE_NAME, buf, opts);
    assert!(resp.is_ok());

    /* GetAsBuffer 不对meta进行过滤 */
    let resp = awos_instance.get_as_buffer::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_ok());
    assert!(resp
        .as_ref()
        .unwrap()
        .meta
        .contains_key("test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* GetAsBuffer 用 Vector 进行过滤 */
    let resp = awos_instance.get_as_buffer(FILE_NAME, vec![]);
    assert!(resp.is_ok());
    assert!(!resp
        .as_ref()
        .unwrap()
        .meta
        .contains_key("test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* GetAsBuffer 用 HashSet 进行过滤 */
    let mut hs = HashSet::new();
    hs.insert("test-key");
    let resp = awos_instance.get_as_buffer(FILE_NAME, hs);
    assert!(resp.is_ok());
    assert!(resp
        .as_ref()
        .unwrap()
        .meta
        .contains_key("test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* Get, 不进行过滤 */
    let resp = awos_instance.get::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_ok() && resp.unwrap().content == String::from_utf8_lossy(BUF));

    /* ListObject, 指定返回为 Vector */
    let resp = awos_instance.list_object(None);
    assert!(resp.is_ok());
    let _v = resp.unwrap(); // Vector with

    /* ListObject, 指定返回为 HashSet */
    let resp = awos_instance.list_object(None);
    assert!(resp.is_ok());
    let _hs = resp.unwrap(); //  HashSet with

    /* 获取 Sign_Url */
    let url = awos_instance.sign_url("A", None);
    println!("{:?}", url);
    let opts = SignedUrlOptions::new("PUT", None);
    let url = awos_instance.sign_url("A", opts);
    println!("{:?}", url);

    /* Head */
    let resp = awos_instance.head(FILE_NAME);
    assert!(resp.is_ok() && resp.unwrap().contains_key("test-key"));

    /* Del */
    let resp = awos_instance.del(FILE_NAME);
    assert!(resp.is_ok());

    /* Check if Del works */
    let resp = awos_instance.get::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_err());
    let resp_err = resp.unwrap_err();
    println!("{:#?}", resp_err);
    // assert!(resp_err.is_not_found()); //should be 404NotFound
    // assert!(!resp_err.is_forbidden()); //other than
    // assert!(!resp_err.is_bad_request());
}

#[test]
fn awos_with_s3_test() {
    let bucket = std::env::var("S3_BUCKET").unwrap_or("s3-test-bucket".to_owned());
    let endpoint = std::env::var("S3_ENDPOINT").unwrap_or("http://127.0.0.1:9000".to_owned());
    let access_key_id = std::env::var("S3_KEY_ID").unwrap_or("minioadmin".to_owned());
    let access_key_secret = std::env::var("S3_KEY_SECRET").unwrap_or("minioadmin".to_owned());

    let awos_instance =
        AwosClient::new_with_s3(endpoint, bucket, access_key_id, access_key_secret).unwrap();

    /* Put Object */
    let buf = BUF.to_owned().into_boxed_slice();
    let mut meta = HashMap::new();
    meta.insert("test-key".to_owned(), "test-val".to_owned());
    let opts = PutOrCopyOptions::new(meta, None, None, None, None);
    let resp = awos_instance.put(FILE_NAME, buf, opts);
    assert!(resp.is_ok());

    /* GetAsBuffer 不对meta进行过滤 */
    let resp = awos_instance.get_as_buffer::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_ok());
    assert!(resp.as_ref().unwrap().meta.contains_key("test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* GetAsBuffer 用 Vector 进行过滤 */
    let resp = awos_instance.get_as_buffer(FILE_NAME, vec![]);
    assert!(resp.is_ok());
    assert!(!resp.as_ref().unwrap().meta.contains_key("test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* GetAsBuffer 用 HashSet 进行过滤 */
    let mut hs = HashSet::new();
    hs.insert("test-key");
    let resp = awos_instance.get_as_buffer(FILE_NAME, hs);
    assert!(resp.is_ok());
    assert!(resp.as_ref().unwrap().meta.contains_key("test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* Get, 不进行过滤 */
    let resp = awos_instance.get::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_ok() && resp.unwrap().content == String::from_utf8_lossy(BUF));

    /* ListObject */
    let resp = awos_instance.list_object(None);
    assert!(resp.is_ok());
    let _v = resp.unwrap(); // Vector with
    println!("{:?}", _v);

    /* 获取 Sign_Url */
    let url = awos_instance.sign_url(FILE_NAME, None);
    println!("{:?}", url);
    let opts = SignedUrlOptions::new("PUT", None);
    let url = awos_instance.sign_url(FILE_NAME, opts);
    println!("{:?}", url);

    /* Head */
    let resp = awos_instance.head(FILE_NAME);
    assert!(resp.is_ok());
    println!("{:#?}", resp);
    // assert!(resp.is_ok() && resp.unwrap().contains_key("test-key"));

    /* Del */
    let resp = awos_instance.del(FILE_NAME);
    assert!(resp.is_ok());

    /* Check if Del works */
    let resp = awos_instance.get::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_err());
    // assert!(resp.unwrap_err().source().unwrap());

    /* 403 */
    let bucket = std::env::var("S3_BUCKET").unwrap_or("s3-test-bucket".to_owned());
    let endpoint = std::env::var("S3_ENDPOINT").unwrap_or("http://127.0.0.1:9000".to_owned());
    let access_key_id = std::env::var("S3_KEY_ID").unwrap_or("minioadmin".to_owned());
    let access_key_secret = std::env::var("S3_KEY_SECRET").unwrap_or("minio".to_owned());
    let awos_instance =  AwosClient::new_with_s3(endpoint, bucket, access_key_id, access_key_secret).unwrap();

    let ret = awos_instance.get::<_,_,Vec<_>>(FILE_NAME, None);
    println!("{:#?}", ret);
}
