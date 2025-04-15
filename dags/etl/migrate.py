from etl.config import load_config
from etl.extract import extract_data
from etl.transform import transform_data, process_large_table
from etl.load import load_data
from etl.s3_utils import save_to_s3, read_from_s3
import os
from dotenv import load_dotenv, dotenv_values


# def set_env_variables():
#     env = {
#         "MYSQL_HOST": "mysqldb",
#         "MYSQL_USER": "root",
#         "MYSQL_PASSWORD": "password",
#         "MYSQL_DB": "testdb",
#         "MARIADB_HOST": "mariadb",
#         "MARIADB_USER": "root",
#         "MARIADB_PASSWORD": "password",
#         "MARIADB_DB": "testdb",
#         "USE_MINIO": True,
#         "S3_BUCKET": "bucket",
#         "AWS_ACCESS_KEY_ID": "root",
#         "AWS_SECRET_ACCESS_KEY": "password",
#         "S3_ENDPOINT_URL": "http://minio:9000",
#         # "AWS_REGION": "us-east-1"  # Uncomment if needed
#     }
#     # Set environment variables
#     for key, value in env.items():
#         os.environ[key] = str(value)
#         print(f"Set {key} to {value}")
#     return True

def set_env_variables(env_path=".env"):
    # Load environment variables from .env file
    abs_path = os.path.abspath(env_path)

    print(f"Trying to load .env file from: {abs_path}")
    if not os.path.exists(abs_path):
        print(f"File {abs_path} does not exist")
        print(f"ERROR: .env file not found, skipping setting the environment variables")
        return False

    load_dotenv(dotenv_path=env_path, override=True)

    # Optionally print out loaded variables for verification
    env = dotenv_values(env_path)
    for key, value in env.items():
        os.environ[key] = str(value)
        print(f"Set {key} to {value}")

    print("Length of env keys: ", len(env.keys()))
    return True


def run_migration(**kwargs):
    print("********* Running migration process")
    env_set_flag = set_env_variables()
    if not env_set_flag:
        print("Environment variables not set. Exiting migration process.")
        raise FileNotFoundError("Required .env not found at /opt/airflow/.env")

    config = load_config()
    if not config:
        print("No config found. Exiting migration process...")
        raise FileNotFoundError("No config provided via dag_run.conf or config.json")
    print("[INFO] Using config:", config)

    tables = config.get("tables", [])

    for table in tables:
        if table.get("source_type") == "mysql":
            source_table = table["source"]
            target_table = table["target"]
            transformations = table.get("transformations", [])

            df = extract_data(source_table)
            df = transform_data(df, transformations)
            load_data(df, target_table)
            save_to_s3(df, f"migrated_data/{target_table}.csv", "csv")
            save_to_s3(df, f"migrated_data/{target_table}.parquet", "parquet")


def run_migration_in_batches(**kwargs):
    print("********* Running migration process")
    env_set_flag = set_env_variables()
    if not env_set_flag:
        print("Environment variables not set. Exiting migration process.")
        raise FileNotFoundError("Required .env not found at /opt/airflow/.env")

    config = load_config()
    if not config:
        print("No config found. Exiting migration process...")
        raise FileNotFoundError("No config provided via dag_run.conf or config.json")
    print("[INFO] Using config:", config)

    tables = config.get("tables", [])

    for table in tables:
        if table.get("source_type") == "mysql":
            source_table = table["source"]
            target_table = table["target"]
            transformations = table.get("transformations", [])
            batch_size = config.get("batch_size", 100000)
            delete_staging = bool(config.get("delete_staging", False))
            process_large_table(source_table, target_table, batch_size, delete_staging, transformations, staging_datatype="parquet")


def run_migration_s3():
    print("********* Running S3 migration process")
    env_set_flag = set_env_variables()
    if not env_set_flag:
        print("Environment variables not set. Exiting migration process.")
        raise FileNotFoundError("Required .env not found at /opt/airflow/.env")

    config = load_config()
    if not config:
        print("No config found. Exiting migration process...")
        raise FileNotFoundError("No config provided via dag_run.conf or config.json")
    print("[INFO] Using config:", config)

    tables = config.get("tables", [])

    if not tables:
        raise RuntimeError("No tables found in dag_run.conf or config.json")

    for table in tables:
        if table.get("source_type") == "s3":  # Only process S3-based sources
            source_file = table["source"]
            target_table = table["target"]
            transformations = table.get("transformations", [])

            # Extract the file extension to determine the datatype
            file_extension = os.path.splitext(source_file)[1].lower()  # Get file extension

            # Map file extensions to target datatypes
            if file_extension == ".csv":
                s3_datatype = "csv"
            elif file_extension == ".parquet":
                s3_datatype = "parquet"
            else:
                raise RuntimeError(f"Unsupported file type: {file_extension}")

            # Read from S3 based on the detected file type
            df = read_from_s3(source_file, s3_datatype)
            df = transform_data(df, transformations)
            load_data(df, target_table)

    return {"message": "S3 to MariaDB migration completed successfully!"}


# if __name__ == "__main__":
#     run_migration()
