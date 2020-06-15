from ftplib import FTP
import s3fs
import urllib
import botocore.vendored.requests.packages.urllib3 as urllib3
import boto3
import zipfile
from zipfile import ZipFile
from io import BytesIO

def lambda_handler(event, context):
    ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/<alias_name>'}
    s3 = s3fs.S3FileSystem(anon=False,s3_additional_kwargs=ExtraArgs)
    
    ftpURL = 'localhost'
    ftpPath = '/test_folder/'
    s3Bucket = 's3-bucket'
    folderName = ''
    filename = 'sample.zip'
    
    with FTP(ftpURL) as ftp:
        ftp.login()
        ftp.cwd(ftpPath)
        s3=boto3.resource('s3')
        http=urllib3.PoolManager()
        if filename in ftp.nlst():
           ftps_url='ftp://'+ftpURL+ftpPath+filename
           folderName=folderName+filename.rsplit('.',1)[0]
           with ZipFile(BytesIO((urllib.request.urlopen(ftps_url)).read())) as my_zip_file:
                for contained_file in my_zip_file.namelist():
                    s3.meta.client.upload_fileobj(my_zip_file.open(contained_file), s3Bucket, folderName+'/'+contained_file,
                                               ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/<alias_name>'})
