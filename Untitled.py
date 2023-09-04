import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import os
from time import time

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet"

os.system(f"wget {url} -O output.parquet")

trips = pq.read_table("output.parquet")

parquet_file = pq.ParquetFile("output.parquet")

engine = create_engine(f"postgresql://{$PG_USER}:{$PG_PASS}@192.168.10.44:{$PG_PORT}/{$PG_DB}")

trips.head(n=0).to_sql(name="yellow_taxi_data",con=engine, if_exists="replace", index=False)

trips.to_sql(name="yellow_taxi_data",con=engine, if_exists="append", index=False)

for batch in parquet_file.iter_batches(batch_size=100000):
    t_start = time()
    batch_df = batch.to_pandas()
    batch_df.to_sql(name="yellow_taxi_data",con=engine, if_exists="append", index=False)
    t_end = time()
    print("iserted next batch %.3f seconds" % (t_end - t_start))
