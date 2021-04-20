use awos_rust::{AwosApi, AwosClient, OSSClient, PutOrCopyOptions, SignUrlOptions};

use std::collections::HashSet;

const FILE_NAME: &str = "rust_oss_sdk_test";

const BUF: &[u8] = "This is just a put test".as_bytes();

#[test]
fn awos_with_oss_test() {
    let bucket = std::env::var("OSS_BUCKET").unwrap();
    let access_key_id = std::env::var("OSS_KEY_ID").unwrap();
    let access_key_secret = std::env::var("OSS_KEY_SECRET").unwrap();

    // endpoint 里的 -internal 会被去除掉。
    let awos_instance = AwosClient::new_with_oss(
        "https://oss-cn-beijing-internal.aliyuncs.com",
        bucket.as_ref(),
        access_key_id,
        access_key_secret,
    )
    .unwrap();

    /* Put Object */
    let buf = BUF.to_owned().into_boxed_slice();
    let opts = PutOrCopyOptions::new(vec![("test-key", "test-val")], None, None, None, None);
    let resp = awos_instance.put(FILE_NAME, buf, opts);
    assert!(resp.is_ok());

    /* GetAsBuffer 不对meta进行过滤 */
    let resp = awos_instance.get_as_buffer::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(resp.is_ok());
    assert!(resp
        .as_ref()
        .unwrap()
        .meta
        .contains_key("x-oss-meta-test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* GetAsBuffer 用 Vector 进行过滤 */
    let resp = awos_instance.get_as_buffer(FILE_NAME, vec![]);
    assert!(resp.is_ok());
    assert!(!resp
        .as_ref()
        .unwrap()
        .meta
        .contains_key("x-oss-meta-test-key"));
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
        .contains_key("x-oss-meta-test-key"));
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    /* ListObject, 指定返回为 Vector */
    let resp = awos_instance.list_object::<_, Vec<_>>(None);
    assert!(resp.is_ok());
    let _v = resp.unwrap(); // Vector with

    /* ListObject, 指定返回为 HashSet */
    let resp = awos_instance.list_object::<_, HashSet<_>>(None);
    assert!(resp.is_ok());
    let _hs = resp.unwrap(); //  HashSet with

    /* 获取 Sign_Url */
    let url = awos_instance.sign_url("A", None);
    println!("{:?}", url);
    let opts = SignUrlOptions::new("PUT", None);
    let url = awos_instance.sign_url("A", opts);
    println!("{:?}", url);

    /* Head */
    let resp = awos_instance.head(FILE_NAME);
    assert!(resp.is_ok() && resp.unwrap().contains_key("x-oss-meta-test-key"));

    /* Del */
    let resp = awos_instance.del(FILE_NAME);
    assert!(resp.is_ok());

    /* Check if Del works */
    let resp = awos_instance.get::<_, _, Vec<_>>(FILE_NAME, None);
    assert!(
        resp.is_err()
            && format!("{}", resp.unwrap_err()).starts_with("GET ERROR: \"SatusCode:404 Not Found")
    );
}
fn default_oss() -> OSSClient<reqwest::blocking::Client> {
    OSSClient::new(
        reqwest::blocking::Client::new(),
        "北京",
        None,
        std::env::var("OSS_BUCKET").unwrap().as_str(),
        std::env::var("OSS_KEY_ID").unwrap(),
        std::env::var("OSS_KEY_SECRET").unwrap(),
    )
}
