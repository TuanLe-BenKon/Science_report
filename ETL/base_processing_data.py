import pandas as pd
from sqlalchemy.types import TypeEngine


def extract_sql_data(engine: TypeEngine, sql_statement: str) -> pd.DataFrame:
    df = pd.read_sql(sql_statement, con=engine)

    return df


def load_df_data(df: pd.DataFrame, des_engine: TypeEngine, table: str) -> None:
    df.to_sql(table, con=des_engine, if_exists="replace", index=False)
    
    print(df.head())


def get_last_row(table_name: str, engine: TypeEngine) -> pd.DataFrame:
    sql_last_row = """
        SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1
    """.format(table_name)
    try:
        df = pd.read_sql(sql_last_row, engine)
    except:
        df = pd.DataFrame()

    return df
