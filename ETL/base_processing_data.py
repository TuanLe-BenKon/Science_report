import pandas as pd
from sqlalchemy import create_engine


def extract_sql_data(conn_url: str, sql_statement: str) -> pd.DataFrame:
    engine = create_engine(conn_url)
    df = pd.read_sql(sql_statement, con=engine)

    return df


def load_df_data(df: pd.DataFrame, des_url: str, table: str) -> None:
    des_engine = create_engine(des_url)
    df.to_sql(table, con=des_engine, if_exists="replace", index=False)

    df_new = pd.read_sql("SELECT * FROM {}".format(table), con=des_engine).head()
    print(df_new)
