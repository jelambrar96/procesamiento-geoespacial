import os

from datetime import datetime
from dateutils import relativedelta
from pathlib import Path

import dlt
import pandas as pd
import requests

from dotenv import load_dotenv


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


# --------------------------------------------------------------------------------------
# Configuración
# --------------------------------------------------------------------------------------
load_dotenv()  # Cargar variables de entorno desde .env si existe

START_DATE = os.getenv("START_DATE", "2025-01-01")
START_DATETIME = datetime.strptime(START_DATE, "%Y-%m-%d")
START_YEAR = START_DATETIME.year
START_MONTH = START_DATETIME.month


def extract_data(url: str):
    """Descarga los datos y los retorna como DataFrame."""
    print(f"[INFO] Descargando {url}")
    df = pd.read_parquet(url)
    return df


# --------------------------------------------------------------------------------------
# Fuente de datos con DLT
# --------------------------------------------------------------------------------------
@dlt.source(name="nyc_taxi_source")
def nyc_taxi_source(start_year: int = 2020, end_year: int = datetime.now().year):
    """DLT source: genera datasets anuales de NYC Taxi (Yellow) desde 2020 hasta hoy."""
    current_datetime = datetime(START_YEAR, START_MONTH, 1)
    while current_datetime <= datetime.now():
        year, month = current_datetime.year, current_datetime.month
        url = BASE_URL.format(year=year, month=month)
        try:
            yield dlt.resource(
                extract_data(url),
                name=f"yellow_tripdata_{year}_{month:02d}",
                write_disposition="append"
            )
        except Exception as e:
            print(f"[ERROR] Error al procesar {url}: {e}")
        current_datetime += relativedelta(months=1)


# --------------------------------------------------------------------------------------
# Flujo principal
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    
    # Configuración dinámica desde variables de entorno
    # s3_config = {
    #     "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    #     "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    #     "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
    #     "bucket_name": os.getenv("AWS_S3_BUCKET"),
    # }

    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_pipeline",
        destination="filesystem",
        dataset_name="nyc_taxi_data"
    )

    load_info = pipeline.run(nyc_taxi_source())
    print(load_info)

