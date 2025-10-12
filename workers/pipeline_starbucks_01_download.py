import os
import re

import dlt
import pandas as pd


from dlt.sources.filesystem import filesystem, read_csv
from dotenv import load_dotenv


load_dotenv()  # Cargar variables de entorno desde .env si existe



def convert_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte los nombres de las columnas de un DataFrame a snake_case."""
    df.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]
    return df



@dlt.source(name="starbucks_source")
def starbucks_source():
    """DLT source: genera datasets anuales de Starbucks desde 2020 hasta hoy."""
    df = pd.read_csv("etc/starbucks/starbucks-locations.csv")
    df = convert_columns_to_snake_case(df)
    yield dlt.resource(
        df,
        name="starbucks_locations",
        write_disposition="replace"
    )




if __name__ == '__main__':


    pipeline = dlt.pipeline(
        pipeline_name='starbucks_pipeline',
        destination='filesystem',
        dataset_name="starbucks_data"
    )

    load_info = pipeline.run(starbucks_source())
    print(load_info)

