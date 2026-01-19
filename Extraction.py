import pandas as pd

# Data Extraction
def run_extraction():
    try:
        data = pd.read_csv(r'rawdata/zipco_transaction.csv')
        print("Data extracted successfully.")
    except Exception as e:
        print(f"Error reading CSV file: {e}")