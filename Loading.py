import pandas as pd
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient



# Data loading
# loading the data
data = pd.read_csv("rawdata/zipco_transaction.csv")
product = pd.read_csv("cleandata/product.csv")
customer = pd.read_csv("cleandata/cutomer.csv")
staff = pd.read_csv("cleandata/staff.csv")
transaction = pd.read_csv("cleandata/transaction.csv")


def run_loading():
    # Data Loading to Azure Blob Storage
    # load environment variables from .env file
    load_dotenv()

connection_str = os.getenv("AZURE_CONNECTION_STR")
container_name = os.getenv("CONTAINER_NAME")

# Create the BlobServiceClient object

blob_service_client = BlobServiceClient.from_connection_string(connection_str)
container_client = blob_service_client.get_container_client(container_name)

# load CSV files to Azure Blob Storage
files = [
    (data, "rawdata/cleaned_zipco_transaction.csv"),
    (product, "cleandata/product.csv"),
    (customer, "cleandata/customer.csv"),
    (staff, "cleandata/staff.csv"),
    (transaction, "cleandata/transaction.csv")
]

for file in files:
    df, blob_name = file
    csv_data = df.to_csv(index=False)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"Uploaded {blob_name} to Azure Blob Storage.")