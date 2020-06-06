from ftplib import FTP_TLS
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

    ftps = FTP_TLS(ftp_url)
    ftps.login('<user_name>','<pwd>')
    ftps.prot_p()
    logger.info('Login Successful')
    ftps.cwd(ftp_path)
    
    logger.info('Files in FTP '+str(ftps.nlst()))
    for file_name in ftps.nlst():
        logger.info('Downloading file: ' +file_name)
        ftps.retrbinary('RETR ' + file_name, s3.open("{}/{}".format(s3Bucket, file_name), 'wb').write)
        logger.info('Download completed: ' +file_name)
