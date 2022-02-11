import pandas as pd

from base_processing_data import extract_sql_data


def process_engery_data(df: pd.DataFrame) -> pd.DataFrame:
    ## remove duplications
    df_result = df.groupby("timestamp")["energy", "power"].mean().reset_index()

    return df_result


def energy_extract_transform(conn_url: str, info: pd.Series) -> pd.DataFrame:
    ## Extract raw data
    sql_get_enegery = """
        SELECT e.id, device_id, d.address, d.alias, user_id, u.name, power, energy, e.timestamp
        FROM public.energy_data as e 
            JOIN public.devices as d ON e.device_id = d.id 
            JOIN public.users as u ON d.user_id = u.id
        WHERE device_id='{}'
        ORDER BY e.timestamp, device_id, address ASC;
        """.format(info["device_id"])

    df_energy_raw = extract_sql_data(conn_url, sql_get_enegery)
    if df_energy_raw.empty:
        print("DataFrame has no data value")
        return

    df_processed_energy = process_engery_data(df_energy_raw)
    df_processed_energy = df_processed_energy.assign(device_id = info["device_id"],
                                                mac_address=info["address"],
                                                alias=info["alias"],
                                                user_id=info["user_id"],
                                                name=info["name"])

    return df_processed_energy