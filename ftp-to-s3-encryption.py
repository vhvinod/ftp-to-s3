from ftplib import FTP
import s3fs
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/<alias_name>'}
    s3 = s3fs.S3FileSystem(anon=False,s3_additional_kwargs=ExtraArgs)
	
    ftp_url = "local"          #provide FTP host
    ftp_path = "/test_folder/" #provide FTP path
    s3Bucket = "s3-bucket"     #provide s3 bucket name
    file_name = "sample.txt"   #provide file name

    with FTP(ftp_url) as ftp:
        ftp.login()
        logger.info('Login Successful')
        ftp.cwd(ftp_path)
        logger.info('Downloading file: ' +file_name)
        ftp.retrbinary('RETR ' + file_name, s3.open("{}/{}".format(s3Bucket, file_name), 'wb').write)
        logger.info('Download completed ' +file_name)
