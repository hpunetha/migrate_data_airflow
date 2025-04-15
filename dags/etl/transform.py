import datetime
import io
import os

import pandas as pd
from etl.config import load_config
from etl.s3_utils import get_s3_client, read_from_s3
from etl.db import get_db_engine
from etl.load import load_data
import uuid


def upload_to_staging(df, staging_prefix, batch_num, target_datatype):
    s3_client = get_s3_client()
    bucket_name = os.getenv("S3_BUCKET").strip()
    filename = f"{staging_prefix}/batch_{batch_num}.{target_datatype}"
    buffer = io.BytesIO()
    if target_datatype == "csv":
        df.to_csv(buffer, index=False)
    else:
        df.to_parquet(buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=filename, Body=buffer.getvalue())
    return filename


def calculate_tenure(joining_date_series):
    today = datetime.date.today()
    return joining_date_series.apply(
        lambda doj: today.year - doj.year - ((today.month, today.day) < (doj.month, doj.day)) if pd.notna(doj) else None
    )


def transform_data(df: pd.DataFrame, transformations: list) -> pd.DataFrame:
    config = load_config()
    transformation_registry = config.get("transformations", {})

    for transform_name in transformations:
        transformation = transformation_registry.get(transform_name)
        if not transformation:
            continue

        column = transformation["column"]
        operation = transformation["operation"]

        if operation == "strip" and column in df.columns:
            output_column = transformation.get("output_column")
            df[output_column] = df[column].str.strip()

        elif operation == "compute_tenure" and column in df.columns:
            output_column = transformation.get("output_column", "years_in_company")
            df[column] = pd.to_datetime(df[column], errors='coerce')
            df[output_column] = calculate_tenure(df[column])

    return df


def process_large_table(source_table, target_table, batch_size, delete_staging, staging_datatype="parquet"):
    engine = get_db_engine("mysql", "source")
    offset = 0
    batch_num = 1
    staging_prefix = f"staging/{source_table}_{target_table}_{uuid.uuid4().hex}"
    s3_client = get_s3_client()
    bucket_name = os.getenv("S3_BUCKET").strip()
    target_datatype = staging_datatype

    while True:
        df = pd.read_sql(f"SELECT * FROM {source_table} LIMIT {batch_size} OFFSET {offset}", engine)
        if df.empty:
            break
        upload_to_staging(df, staging_prefix, batch_num, target_datatype)
        offset += batch_size
        batch_num += 1

    for obj in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=staging_prefix).get("Contents", []):
        df = read_from_s3(obj["Key"], target_datatype)
        load_data(df, target_table, batch_size)

    if delete_staging:
        for obj in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=staging_prefix).get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    else:
        print(f"Skipping deletion from staging area for {staging_prefix}")