## AWOS integration tests

#### Warning - these can incur charges on AWS/OSS

These tests talk to real AWS/OSS services and your account may be charged for the actions taken.

Also, if tests fail they can leave items in your AWS/OSS account, such as OSS buckets.

<!-- #### Running the tests against AWS/OSS

In this directory, `tests`, you can run all tests with `cargo test --features all`.

Specific service tests can be run using their feature flags.  To run the OSS tests: `cargo test --features oss`.

To run multiple service tests, add the feature flags: `cargo test --features "oss s3"`.

To run specific tests, your may wish to use cargo test's `--test <testname>` filter. -->
