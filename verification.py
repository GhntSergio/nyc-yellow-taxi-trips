#%%
from google.cloud import storage
import pyarrow.parquet as pq
import gcsfs

#%%
# Set project-specific variables
PROJECT_ID = "vde-datawarehouse-477514"
BUCKET_NAME = f"nyc-yellow-taxi-trips-data-buck"
GCS_FOLDER = "dataset/trips/"

#Iitilize BQ, GCS Clients and GCS file system
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
fs = gcsfs.GCSFileSystem(project=PROJECT_ID)

t_rows = 0

# List alll parquet file
blobs = bucket.list_blobs(prefix=GCS_FOLDER)
parquet_files = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

# %%
for file in parquet_files:
    file_path = f"gs://{BUCKET_NAME}/{file}"
    with fs.open(file_path) as f:
        table = pq.read_table(f)
        t_rows += table.num_rows

# %%
print(f"Total number of rows: {t_rows}")

# %%