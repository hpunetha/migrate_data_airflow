import pandas as pd
from etl.db import get_db_engine


def load_data(df: pd.DataFrame, target_table: str, batch_size: int = 100000):
    engine = get_db_engine("mariadb", "target")
    with engine.connect() as conn:
        if not engine.dialect.has_table(conn, target_table):
            df.head(0).to_sql(target_table, engine, if_exists="fail", index=False)

    for start in range(0, len(df), batch_size):
        end = start + batch_size
        df[start:end].to_sql(target_table, engine, if_exists="append", index=False)
