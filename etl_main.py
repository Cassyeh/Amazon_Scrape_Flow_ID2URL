import time
from src.extract_dataset import extract_huggingface_dataset
from utils.file_handling import load_hf_dataset_as_parquet
from src.transform_data import transform_dataset


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

end_time = time.time()

print(f"Time taken for extraction and transformation: {end_time - start_time} seconds")