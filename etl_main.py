import time
from src.extract_dataset import extract_huggingface_dataset
from utils.file_handling import load_hf_dataset_as_parquet, polars_to_parquet
from src.transform_data import transform_dataset
from src.load_data_to_db import load_parquet_to_duckdb

# Measure the time taken
start_time = time.time()

# Step 1: Extract
dataset_name = "kevykibbz/Amazon_Customer_Review_2023"
dataset = extract_huggingface_dataset(dataset_name)

# Turn df to parquet file format
parquet_dataset_path = "./data/output/amazon_reviews_table.parquet"
parquet_path = load_hf_dataset_as_parquet(dataset, parquet_dataset_path)

# Step 2: Transform
df_cleaned = transform_dataset(parquet_path)

# Step 3: Load
parquet_path = polars_to_parquet(df_cleaned, "./data/output/amazon_reviews_df_cleaned.parquet")
database_path = "./data/output/amazon_sales_db.duckDB"
table_name = "amazon_reviews"
load_parquet_to_duckdb(database_path, table_name, parquet_path)

end_time = time.time()

# Calculate how long it takes to run the script
print(f"Time taken for extraction, transformation and loading of data: {end_time - start_time} seconds")