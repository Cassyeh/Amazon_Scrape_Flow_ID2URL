import time
from src.extract_dataset import extract_huggingface_dataset
from src.transform_data import transform_dataset
from utils.file_handling import load_hf_dataset_as_parquet, polars_to_parquet
from src.load_data_to_db import load_parquet_to_duckdb
from utils.create_duckdb_table import create_grouped_table
from utils.write_duckdb_to_xls import export_table_to_excel
from utils.insert_to_table import insert_product_url_from_web

# Measure the time taken
start_time = time.time()

# # Step 1: Extract
dataset_name = "kevykibbz/Amazon_Customer_Review_2023"
dataset = extract_huggingface_dataset(dataset_name)

# # Turn df to parquet file format
parquet_dataset_path = "./data/output/amazon_reviews_table.parquet"
parquet_path = load_hf_dataset_as_parquet(dataset, parquet_dataset_path)

# # Step 2: Transform
df_cleaned = transform_dataset(parquet_path)

# # Step 3: Load
parquet_path = polars_to_parquet(df_cleaned, "./data/output/amazon_reviews_df_cleaned.parquet")
database_path = "./data/output/amazon_sales_db.duckDB"
table_name = "amazon_reviews"
load_parquet_to_duckdb(database_path, table_name, parquet_path)

# Post-processing
# Create count of top product table
top_product_table = create_grouped_table(
    database_path, "top_products_count", group_column="asin", limit=10000, 
    filter_column='rating', filter_operator = '=', filter_value=5)
excel_output_path = "./data/output/"
export_table_to_excel(database_path, top_product_table, excel_output_path)

input_table = top_product_table   # table with asin
output_table = "product_url_table"
insert_product_url_from_web(database_path, input_table, output_table)
export_table_to_excel(database_path, output_table, excel_output_path)

end_time = time.time()

# Calculate how long it takes to run the script
print(f"Time taken for extraction, transformation, loading and post-processing of data: {end_time - start_time} seconds")