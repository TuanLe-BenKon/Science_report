import pandas as pd
from sqlalchemy.types import TypeEngine


from base_processing_data import get_last_row, extract_sql_data, load_df_data

SOURCE_TABLE = "energy_data"
DEST_TABLE = "transformed_energy"


def process_engery_data(df: pd.DataFrame) -> pd.DataFrame:
    ## remove duplications
    df_result = df.groupby("timestamp")["energy", "power"].mean().reset_index()

    return df_result



def generate_sql_statement(df_last_row: pd.DataFrame) -> str:
    sql_statement = """
        SELECT e.id, device_id, d.address, d.alias, user_id, u.name, power, energy, e.timestamp
        FROM public.{} as e 
            JOIN public.devices as d ON e.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        ORDER BY e.timestamp, device_id, address ASC;
    """.format(SOURCE_TABLE)
    if not df_last_row.empty:
        sql_statement = """
            SELECT e.id, device_id, d.address, d.alias, user_id, u.name, power, energy, e.timestamp
            FROM public.{} as e 
                JOIN public.devices as d ON e.device_id = d.id 
                JOIN public.users as u ON d.user_id = u.id
            WHERE e.timestamp > '{}'
            ORDER BY e.timestamp, device_id, address ASC;
        """.format(SOURCE_TABLE, df_last_row["timestamp"][0])

    return sql_statement

def energy_ETL(conn_engine: TypeEngine, des_engine: TypeEngine) -> bool:
    last_row = get_last_row(DEST_TABLE, des_engine)
    energy_sql = generate_sql_statement(last_row)

    df_energy_raw = extract_sql_data(conn_engine, energy_sql)
    if df_energy_raw.empty:
        print("DataFrame has no data value")
        return

    df_processed_energy = process_engery_data(df_energy_raw)
    df_processed_energy = df_processed_energy.assign(device_id = df_energy_raw["device_id"],
                                            mac_address=df_energy_raw["address"],
                                            alias=df_energy_raw["alias"],
                                            user_id=df_energy_raw["user_id"],
                                            name=df_energy_raw["name"])

    load_df_data(df_processed_energy, des_engine, DEST_TABLE)
    return True