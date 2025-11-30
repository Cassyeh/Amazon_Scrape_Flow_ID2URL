import os
import polars as pl


def load_hf_dataset_as_parquet(dataset, parquet_dataset_path: str = "./data/output/dataset.parquet", batch_size: int = 500_000):
    """
    Load a HuggingFace dataset split as a Polars DataFrame.
    If the parquet file exists, read it. Otherwise, download the dataset and save it as parquet.

    Args:
        dataset: HuggingFace dataset.
        parquet_dataset_path (str): Path to save/read the parquet file.
        batch_size (int): Batch size when writing parquet.

    Returns:
        parquet_dataset_path: path to saved parquet file.
    """

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(parquet_dataset_path), exist_ok=True)

    # Download and save parquet if it doesn't exist
    if not os.path.isfile(parquet_dataset_path):
        print(f"Parquet file {parquet_dataset_path} does not exist.")
        print(f"Writing data to Parquet in batches of {batch_size}...")
        dataset['train'].to_parquet(parquet_dataset_path, batch_size=batch_size)
        print(f"Parquet file written successfully: {parquet_dataset_path}")
    else:
        print(f"Parquet file {parquet_dataset_path} already exists. Skipping download.")

    return parquet_dataset_path

""" EXAMPLE USAGE
from utils.file_handling import load_hf_dataset_as_parquet

dataset_name = "kevykibbz/Amazon_Customer_Review_2023"
parquet_path = "./data/output/amazon_reviews_table.parquet"

parquet_path = load_hf_dataset_as_parquet(dataset_name, parquet_dataset_path=parquet_path) """


def polars_to_parquet(df: pl.DataFrame, parquet_df_file_path: str = "./data/output/df_file.parquet"):
    """
    Save a Polars DataFrame to a Parquet file.

    Args:
        df (pl.DataFrame): The Polars DataFrame to save.
        parquet_df_file_path (str): Path to the Parquet file to create.

    Returns:
        str: Path to the saved Parquet file.
    """

    # Ensure the file output directory exists
    os.makedirs(os.path.dirname(parquet_df_file_path), exist_ok=True)

    # Write df to parquet if it doesn't exist
    if not os.path.isfile(parquet_df_file_path):
        print(f"Parquet file {parquet_df_file_path} does not exist.")
        df.write_parquet(parquet_df_file_path, compression="snappy")
        print(f"✔ DataFrame saved to Parquet successfully: {parquet_df_file_path}")
    else:
        print(f"Parquet file {parquet_df_file_path} already exists. Skipping download.")

    return parquet_df_file_path

# EXAMPLE USAGE
# polars_to_parquet(df, "data/my_table.parquet")