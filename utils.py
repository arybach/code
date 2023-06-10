import boto3
import os

def download_from_s3(s3_path):
    """
    Downloads a file from S3 to a local path and returns the local path.
    :param s3_path: str, the S3 path to the file to download
    :return: str, the local path where the file was downloaded to
    """
    # Split the S3 path into bucket and key components
    s3_components = s3_path.split('/')
    bucket_name = s3_components[2]
    key = '/'.join(s3_components[3:])

    # keeping local file structure the same as in s3
    local_dir = os.path.dirname(key)

    # Create the local directory if it does not exist
    os.makedirs(local_dir, exist_ok=True)

    # Extract the local file name from the key and join it to the local directory
    local_file = os.path.join(local_dir, os.path.basename(key))

    # Download the file from S3
    # s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.download_file(key, local_file)
    
    # Return the local path where the file was downloaded to
    return local_file

##############################################################################################

def upload_to_s3(local_path, bucket_name):
    """
    Uploads a file to S3 and returns the S3 path where the file was uploaded to.
    :param local_path: str, the local path to the file to upload
    :param bucket_name: str, the name of the S3 bucket to upload the file to
    :param aws_access_key_id: str, the AWS access key ID to use for authentication
    :param aws_secret_access_key: str, the AWS secret access key to use for authentication
    :return: str, the S3 path where the file was uploaded to
    """
    # s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3 = boto3.resource('s3')
    # local_path should be w/o / at the beginning
    s3_path = f's3://{bucket_name}/{local_path}'
    s3.Object(bucket_name, local_path).upload_file(local_path)
    return s3_path

##############################################################################################

def list_files_in_folder(bucket_name, folder_path):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=folder_path,
        Delimiter='/'
    )

    folders = {}
    for common_prefix in response.get('CommonPrefixes', []):
        folder_name = common_prefix['Prefix'].split('/')[-2]
        folders[folder_name] = []

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=common_prefix['Prefix']
        )

        for obj in response['Contents']:
            file_name = obj['Key'].split('/')[-1]
            folders[folder_name].append(file_name)

    return folders
