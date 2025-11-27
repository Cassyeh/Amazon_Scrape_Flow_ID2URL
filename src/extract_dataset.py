import os
import time
from datasets import load_dataset

def extract_huggingface_dataset(dataset_name: str, dataset_folder: str = './data/raw', retries: int = 5, wait_time: int = 7):
    """
    Load a Hugging Face dataset with caching to disk.
    
    Args:
        dataset_name (str): Name of the Hugging Face dataset (e.g., "kevykibbz/Amazon_Customer_Review_2023").
        dataset_folder (str, optional): Folder where the dataset will be cached. Defaults to './data/raw'.
        retries (int, optional): Number of download retries if the download fails. Defaults to 5.
        wait_time (int, optional): Seconds to wait between retries. Defaults to 7.
    
    Returns:
        datasets.DatasetDict: The loaded dataset.
    """
    
    dataset_path = os.path.join(dataset_folder, dataset_name.replace('/', '_') + "_data")

    # Ensure folder exists
    if not os.path.exists(dataset_folder):
        print(f"Creating folder: {dataset_folder}")
        os.makedirs(dataset_folder)
    else:
        print(f"Folder exists: {dataset_folder}")

    dataset = None

    # Load from cache or download
    if not os.path.exists(dataset_path):
        print("Dataset not found in cache. Downloading...")
        for attempt in range(1, retries + 1):
            try:
                dataset = load_dataset(dataset_name)
                print("Dataset loaded successfully!")
                dataset.save_to_disk(dataset_path)
                break
            except Exception as e:
                print(f"Attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise RuntimeError(f"Failed to download dataset after {retries} attempts.")
    else:
        print("Dataset found in cache. Loading from cache.")
        dataset = load_dataset(dataset_path)

    # Check dataset content
    if dataset is not None:
        print("Dataset loaded successfully.")
        try:
            num_rows = len(dataset['train'])
            print(f"Number of rows in the 'train' split: {num_rows}")
        except KeyError:
            print("Warning: 'train' split not found in the dataset.")
    
    return dataset

""" EXAMPLE USAGE
from extract_dataset import extract_huggingface_dataset

dataset_name = "kevykibbz/Amazon_Customer_Review_2023"
dataset = extract_huggingface_dataset(dataset_name)
"""
