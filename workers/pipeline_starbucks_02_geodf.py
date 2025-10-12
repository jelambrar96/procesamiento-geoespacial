import json
import os


import geopandas as gpd
import pandas as pd
import shapely

from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()  


"""
AWS_ACCESS_KEY: $MINIO_ACCESS_KEY
AWS_SECRET_KEY: $MINIO_SECRET_KEY
MINIO_SERVER: "http://minio:9000"
BUCKET_NAME: $MINIO_BUCKET_NAME
"""


AWS_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_SERVER = os.getenv("MINIO_SERVER", "http://minio:9000")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "dlt-bucket")


POSTGIS_PASSWORD = os.getenv('POSTGIS_PASSWORD', None)
POSTGIS_USER = os.getenv('POSTGIS_USER', None)
POSTGIS_DB = os.getenv('POSTGIS_DB', None)
POSTGIS_HOST = os.getenv('POSTGIS_HOST', None)
POSTGIS_PORT = os.getenv('POSTGIS_PORT', None)


STARBUCKS_FILE_KEY = "starbucks_data/starbucks_locations/"


# Configuración de las credenciales y endpoint de MinIO
s3_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_SERVER},
}

# print(json.dumps(s3_options, indent=4))

# Ruta al directorio dentro del bucket
path = f"s3://{MINIO_BUCKET_NAME}/{STARBUCKS_FILE_KEY}"

# print(path)

# Leer todos los Parquet del directorio en un solo DataFrame
df = pd.read_parquet(path, storage_options=s3_options)

print(df.head())


def generate_point_geometry(row):
    """
    generata a point from row[latitude], row[longitude]
    """
    point = shapely.geometry.Point(row['longitude'], row['latitude'])
    return point


df['geometry'] = df.apply(generate_point_geometry, axis=1)


gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
del gdf['latitude']
del gdf['longitude']


POSTGIS_URI = f"postgresql://{POSTGIS_USER}:{POSTGIS_PASSWORD}@{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
engine = create_engine(POSTGIS_URI)

gdf.to_postgis("starbucks_locations", engine, if_exists='replace', index=False)
print("DataFrame cargado en PostGIS correctamente.")

