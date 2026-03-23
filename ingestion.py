import os
import shutil
import json

def ingest_data():
    print("--- Starting Data Ingestion Pipeline ---")
    
    # 1. Load configuration paths
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    source_dir = config['source_folder']
    lake_dir = config['data_lake_folder']

    # 2. Ensure Data Lake directory exists (creates it if missing)
    if not os.path.exists(lake_dir):
        os.makedirs(lake_dir)
        print(f"Created Data Lake directory at: {lake_dir}")

    # 3. Simulate Ingestion (Extracting from Source, Loading to Data Lake)
    files_ingested = 0
    for filename in os.listdir(source_dir):
        if filename.endswith(".csv"):
            source_file = os.path.join(source_dir, filename)
            destination_file = os.path.join(lake_dir, filename)
            
            # Copy the file to simulate moving data to a Data Lake
            shutil.copy2(source_file, destination_file)
            print(f"  -> Successfully ingested: {filename}")
            files_ingested += 1
    
    if files_ingested == 0:
        print("Warning: No CSV files found in the source_data folder. Did you download them?")
    else:
        print(f"--- Ingestion Complete! {files_ingested} files moved to the Data Lake. ---")

if __name__ == "__main__":
    ingest_data()