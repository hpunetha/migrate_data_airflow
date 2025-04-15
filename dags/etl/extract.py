import pandas as pd
from etl.db import get_db_engine


def extract_data(source_table: str) -> pd.DataFrame:
    engine = get_db_engine("mysql", "source")
    return pd.read_sql(f"SELECT * FROM {source_table}", engine)
