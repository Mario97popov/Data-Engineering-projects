import boto3

from ETL_Excercise.config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def get_s3_client_and_storage_options():
    s3 = boto3.client(
        "s3",
        aws_access_key_id= AWS_ACCESS_KEY_ID,
        aws_secret_access_key= AWS_SECRET_ACCESS_KEY,
    )

    storage_options = {
        "key": s3._request_signer._credentials.access_key,
        "secret": s3._request_signer._credentials.secret_key
    }

    return s3, storage_options