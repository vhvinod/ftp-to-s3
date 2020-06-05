# AWS Lambda FTP Function
## Getting Started
Simple python script to stream the file from ftp server to s3 bucket
### Problem
AWS Lambda has a limitation of providing only `500MB` of disk space per instance. If you're processing file are `<500MB` in size, than Lambda will work perfectly fine.
In case file size is `>500MB`, then we can't download the file in /tmp location in AWS lambda.

You’ve hit the limit of Lambda. What is the solution..?
### Solution?
Do not write to `/tmp` location, stream directly to AWS S3 bucket.
Stream the file from FTP and write its contents on the fly using Python to S3 bucket.

This method does not use up disk space and therefore is not limited by size.
#### Note
AWS Execution time limit has a maximum of 15 minutes so can you process your HUGE files in this amount of time? You can only know by testing.
This method does not use up disk space and therefore is not limited by size.
## Versions
```
Python 3.7
```
## To do list
- [X] Stream the file from ftp to s3 without ftp credentials (***ftp-to-s3.py***)
- [ ] Stream the file from ftp to s3 without ftp credentials and with s3 encryption
- [ ] Stream the file from ftp to s3 with ftp credentials

## To get Source Code
git clone https://github.com/vhvinod/ftp-to-s3.git
