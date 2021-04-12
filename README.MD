# Layout 简述
## AWOS
### awos.rs
定义了对外接口 - AwosApi 与.
实现了这个接口的AwosClientAwosClient 封装了了OSS和S3。
### oss.rs, aws.rs
oss与s3对AwosApi的具体实现.
分别对oss_sdk 和 rusoto_s3进行了封装。
### types.rs, errors.rs
定义了 AwosApi 的一些参数、返回值以及错误类型。

## OSS_SDK
HTTP Requests 的封装。

