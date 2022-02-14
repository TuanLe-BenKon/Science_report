import os

from sqlalchemy import create_engine
from dotenv import load_dotenv

from processing_activities import activities_ETL
from processing_energy import energy_ETL
from processing_sensor import sensor_ETL

load_dotenv()

if __name__=="__main__":
    conn_url = os.getenv("SOURCE_DATABASE_URL")
    conn_engine = create_engine(conn_url)
    
    des_url = os.getenv("DEST_DATABASE_URL")
    des_engine = create_engine(des_url)

    activities_ETL(conn_engine, des_engine)
    energy_ETL(conn_engine, des_engine)
    sensor_ETL(conn_engine, des_engine)