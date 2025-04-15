import os
from sqlalchemy import create_engine
from etl.config import load_config
from dotenv import load_dotenv

load_dotenv()


def get_db_engine(db_type: str, role: str):
    config = load_config()

    db_config = None
    if role == "source":
        db_config = next((ds for ds in config["data_sources"] if ds["type"] == db_type), None)
    elif role == "target":
        db_config = next((tg for tg in config["targets"] if tg["type"] == db_type), None)

    if not db_config:
        raise ValueError(f"No {role} database configuration found for {db_type}")

    host = os.getenv(db_config["host"], db_config["host"])
    user = os.getenv(db_config["user"], db_config["user"])
    password = os.getenv(db_config["password"], db_config["password"])
    database = os.getenv(db_config["database"], db_config["database"])

    return create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
