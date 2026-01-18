import pandas as pd
import fsspec
import s3fs

from botocore.exceptions import BotoCoreError

from ETL_Excercise.config.s3_uutils import get_s3_client_and_storage_options

def extract_s3(bucket_name: str, folder: str) -> list:
    s3, storage_options = get_s3_client_and_storage_options()
    mapping = {"sales": None, "customer": None, "product": None, "shipping": None}

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        contents = response.get("Contents", [])
        if not contents:
            raise FileNotFoundError(f"No files found in {bucket_name}/{folder}")
    except BotoCoreError as e:
        print(f"S3 access error for {bucket_name}/{folder}")
        raise

    for obj in contents:
        key = obj["Key"]

        if not key.lower().endswith(".csv"):
            continue

        s3_path = f"s3://{bucket_name}/{key}"

        try:
            df = pd.read_csv(s3_path, storage_options=storage_options)
        except Exception as e:
            print(f"Skipping {s3_path}: {e}")
            continue

        for name in mapping.keys():
            if name in key.lower():
                mapping[name] = df
                break

    return [mapping["sales"], mapping["customer"], mapping["product"], mapping["shipping"]]