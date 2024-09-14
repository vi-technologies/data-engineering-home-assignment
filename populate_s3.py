import os
import boto3

def upload_file_to_s3(local_directory, bucket_name):
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
    else:
        s3_client = boto3.client('s3')

    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_directory)

            try:
                s3_client.upload_file(local_file_path, bucket_name, relative_path)
                print(f'Uploaded {local_file_path} to s3://{bucket_name}/{relative_path}')
            except Exception as e:
                print(f'Failed to upload {local_file_path} to s3://{bucket_name}/{relative_path}. Error: {e}')

if __name__ == "__main__":
    current_directory = os.path.dirname(__file__)
    local_folder = os.path.join(current_directory, 's3_contents')
    bucket_name = 'data-engineer-assignment-yuval-dror'    # Replace with your S3 bucket name

    upload_file_to_s3(local_folder, bucket_name)
