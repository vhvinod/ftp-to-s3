from ftplib import FTP_TLS
import s3fs
import boto3
import json
from io import BytesIO
import zipfile
from zipfile import ZipFile

def lambda_handler(event, context):
    ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/<alias_name>'}
    s3 = s3fs.S3FileSystem(anon=False,s3_additional_kwargs=ExtraArgs)
    
    s3Bucket ="s3-bucket"
    file_name = "test.zip"
    
    s3=boto3.resource('s3')
    zip_obj = s3.Object(bucket_name=s3Bucket, key=file_name)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    for file in z.namelist():
        file_info = z.getinfo(file)
        s3.meta.client.upload_fileobj(
            z.open(file),
            Bucket=s3Bucket,
            Key=file,
            ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'alias/<alias_name>'})
