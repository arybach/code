from time import sleep
import os
from prefect_aws import S3Bucket, AwsCredentials

def create_aws_creds_block():
    aws_creds_obj = AwsCredentials(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    aws_creds_obj.save(name="aws-creds", overwrite="True")

def create_aws_bucket_block():
    aws_creds = AwsCredentials.load("aws-creds")                                    
    s3_bucket_obj = S3Bucket(
        bucket_name="mlops-prefect", credentials=aws_creds
    )
    s3_bucket_obj.save(name="mlops-prefect", overwrite=True)


if __name__=='__main__':
    create_aws_creds_block()
    sleep(10)
    create_aws_bucket_block()