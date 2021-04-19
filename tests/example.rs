use awos_rust::{AwosApi, AwosClient, OSSClient, PutOrCopyOptions};

use std::collections::HashSet;

const FILE_NAME: &str = "rust_oss_sdk_test";
// const FILE_NAMES: &[&str] = &[
//     "rust_oss_sdk_test_1",
//     "rust_oss_sdk_test_2",
//     "rust_oss_sdk_test_3",
//     "rust_oss_sdk_test_4",
//     "rust_oss_sdk_test_5",
// ];
const BUF: &[u8] = "This is just a put test".as_bytes();
// const BUFS: &[&[u8]] = &[
//     "An".as_bytes(),
//     "Array".as_bytes(),
//     "Of".as_bytes(),
//     "Put".as_bytes(),
//     "Test".as_bytes(),
// ];

#[test]
fn awos_with_oss_test() {
    let bucket = std::env::var("OSS_BUCKET").unwrap();
    let access_key_id = std::env::var("OSS_KEY_ID").unwrap();
    let access_key_secret = std::env::var("OSS_KEY_SECRET").unwrap();

    let awos_instance = AwosClient::new_with_oss(
        "北京",
        None,
        bucket.as_ref(),
        access_key_id,
        access_key_secret,
    )
    .unwrap();
    // Put
    let buf = BUF.to_owned().into_boxed_slice();
    let opts = PutOrCopyOptions::new([("test-key", "test-val")].to_vec(), None, None, None, None);
    let resp = awos_instance.put(FILE_NAME, buf, opts);
    assert!(resp.is_ok());
    println!("{:#?}", resp);

    let mut hs = HashSet::new();
    hs.insert("test-key");
    let resp = awos_instance.get_as_buffer(FILE_NAME, None);
    assert!(resp.is_ok());
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    let resp = awos_instance.list_object::<_, Vec<_>>(None);
    assert!(resp.is_ok());

    let url = awos_instance.sign_url("A", None);
    println!("{:?}", url);

    let resp = awos_instance.head(FILE_NAME);
    assert!(resp.is_ok() && resp.unwrap().contains_key("x-oss-meta-test-key"));

    let resp = awos_instance.del(FILE_NAME);
    assert!(resp.is_ok());

    let resp = awos_instance.get(FILE_NAME, None);
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

#[test]
fn oss_smoke_test() {
    let oss_instance = default_oss();
    let buf = BUF.to_owned().into_boxed_slice();
    let opts = PutOrCopyOptions::new([("test-key", "test-val")].to_vec(), None, None, None, None);
    let resp = oss_instance.put(FILE_NAME, buf, opts);
    assert!(resp.is_ok());

    let mut hs = HashSet::new();
    hs.insert("test-key");
    let resp = oss_instance.get_as_buffer(FILE_NAME, None);
    assert!(resp.is_ok());
    let resp_content = std::pin::Pin::into_inner(resp.unwrap().content);
    assert_eq!(*BUF, *resp_content);

    let resp = oss_instance.list_object::<_, Vec<_>>(None);
    assert!(resp.is_ok());

    let resp = oss_instance.head(FILE_NAME);
    assert!(resp.is_ok() && resp.unwrap().contains_key("x-oss-meta-test-key"));

    let resp = oss_instance.del(FILE_NAME);
    assert!(resp.is_ok());

    let resp = oss_instance.get(FILE_NAME, None);
    assert!(
        resp.is_err()
            && format!("{}", resp.unwrap_err()).starts_with("GET ERROR: \"SatusCode:404 Not Found")
    );
}
