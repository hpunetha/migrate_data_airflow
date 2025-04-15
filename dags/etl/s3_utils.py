import os
import io
import boto3
import pandas as pd
from dotenv import load_dotenv

# load_dotenv()

def get_use_minio():
    """
    Check if the environment variable USE_MINIO is set to True.
    """

    print("USE_MINIO env Value is - ", os.getenv("USE_MINIO", ""))
    use_minio = bool(os.getenv("USE_MINIO", "").strip()) == True
    return use_minio


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL").strip() if get_use_minio() else None,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID").strip(),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY").strip(),
        region_name=os.getenv("AWS_REGION").strip() if not get_use_minio() else None,
    )


def save_to_s3(df: pd.DataFrame, filename: str, target_datatype: str = "csv"):
    s3_client = get_s3_client()
    bucket_name = os.getenv("S3_BUCKET").strip()

    if target_datatype == "csv":
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=filename, Body=buffer.getvalue(), ContentType="text/csv")
    elif target_datatype == "parquet":
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=filename, Body=buffer.getvalue(),
                             ContentType="application/parquet")


def read_from_s3(file_path: str, target_datatype: str = "csv") -> pd.DataFrame:
    s3_client = get_s3_client()
    bucket_name = os.getenv("S3_BUCKET").strip()
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)

    if target_datatype == "csv":
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    elif target_datatype == "parquet":
        return pd.read_parquet(io.BytesIO(obj['Body'].read()))
