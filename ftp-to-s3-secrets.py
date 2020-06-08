from ftplib import FTP_TLS
import s3fs
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/<alias_name>'}
    s3 = s3fs.S3FileSystem(anon=False,s3_additional_kwargs=ExtraArgs)
    
    secret_name = "<secret_name>" #provide secret name from AWS secrets manager
    region_name = "<region_name>" #provide region name
    
    ftp_url = "localhost"         #provide FTP host
    ftp_path = "/test_folder/"    #provide FTP path
    s3Bucket = "s3-bucket"        #provide s3 bucket name
    file_name = "sample.txt"      #provide file name
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    
    secret_dict = json.loads(secret)

    ftps = FTP_TLS(ftp_url)
    ftps.login(secret_dict['username'],secret_dict['password'])
    ftps.prot_p()
    logger.info('Login Successful')
    ftps.cwd(ftp_path)
    
    ftps.retrbinary('RETR ' +file_name , s3.open("{}/{}".format(s3Bucket, file_name), 'wb').write)
    logger.info('Download completed: ' +file_name)
