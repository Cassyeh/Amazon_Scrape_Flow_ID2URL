import time
import pandas as pd

def transform_dataset(dataset, split: str = 'train'):
    """
    Transform a Hugging Face reviews dataset into a cleaned pandas DataFrame.
    
    Args:
        dataset (datasets.DatasetDict): Hugging Face dataset object
        split (str, optional): Which split to use (default 'train')
    
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    start_time = time.time()

    # Convert to pandas DataFrame
    df = dataset[split].to_pandas()

    # Drop duplicates 
    df_no_duplicate = df.drop_duplicates()

    print("-" * 30)
    print("Shape of data before removing duplicates:", df.shape)
    print("Shape of data after removing duplicates:", df_no_duplicate.shape)
    print("-" * 30)
    
    # Convert milliseconds timestamp to seconds and then to datetime YYYY-MM-DD 09:57:33.520000
    df_no_duplicate["datetime_of_review"] = pd.to_datetime(df_no_duplicate["timestamp"] / 1000, unit='s')

    # Filter verified purchases
    df_purchase_true = df_no_duplicate[df_no_duplicate['verified_purchase'] == True]

    # Clean up text: Convert to lowercase and remove extra spaces
    df_cleaned_texts = df_purchase_true.copy()
    df_cleaned_texts['title'] = df_purchase_true['title'].str.lower().str.replace(r'\s+', ' ', regex=True).str.strip()
    df_cleaned_texts['text'] = df_purchase_true['text'].str.lower().str.replace(r'\s+', ' ', regex=True).str.strip()
    

    # Optional: print the number of rows
    print(f"Number of rows after transformation: {len(df_purchase_true)}")

    end_time = time.time()
    print(f"Time taken for transformation: {end_time - start_time:.2f} seconds")

    return df

""" EXAMPLE USAGE
from datasets import load_dataset
dataset_name = "kevykibbz/Amazon_Customer_Review_2023"
dataset = load_dataset(dataset_name)
df_cleaned = transform_dataset(dataset, split='train') """