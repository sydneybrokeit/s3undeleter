# S3 Undeleter
Remove delete markers from a versioned S3 bucket

## How to build
* install dependencies
  * `go get -u github.com/aws/aws-sdk-go`
  * `go get -u github.com/sirupsen/logrus`
* build
  * go build .
  
## Running
### Flags
* `region`
* `threads` - threads to run for parsing returns
* `bucket`
* `requestThreads`
* `prefix` - s3 prefix to search