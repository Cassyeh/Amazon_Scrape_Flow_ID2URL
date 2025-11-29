import duckdb
import polars as pl


def transform_dataset(parquet_dataset_path):
    """
    Transform a Polars Dataframe into a cleaned DataFrame.
    
    Args:
        parquet_dataset_path: file where the df is stored as parquet
    
    Returns:
        polars.DataFrame: Transformed DataFrame
    """
    table_name = "data_no_dupes"

    # Connect to DuckDB
    con = duckdb.connect()  # in-memory database

    # Check the count of data to ensure number of rows matches the number of rows in dataset  == 33913690
    num_rows_df = con.execute(
    "SELECT COUNT(*) FROM parquet_scan(?)", 
    [parquet_dataset_path]).fetchone()[0]

    # Get schema (column names and types)
    schema_info = con.execute(f"DESCRIBE SELECT * FROM '{parquet_dataset_path}'").fetchall()

    # Count columns
    num_columns_df = len(schema_info)

    print(f"Number of rows: {num_rows_df}")
    print(f"Number of columns: {num_columns_df}")

    # Deduplicate directly from Parquet path and write to a new Parquet file
    con.execute(f"""
    CREATE TABLE {table_name} AS
    SELECT DISTINCT * 
    FROM '{parquet_dataset_path}'
    """)
    print(f"Table {table_name} created without duplicates")

    # Count the number of rows after deduplication == 33567657
    num_rows_no_dupes_df = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Number of rows after duplicates removed: {num_rows_no_dupes_df}")

    # Build a query that counts nulls for each column
    null_count_exprs = [
        f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}"
        for col, _type, *_ in schema_info
    ]
    query = f"SELECT {', '.join(null_count_exprs)} FROM '{table_name}';"

    # Count null values per column
    result_nulls = con.execute(query).fetchone()

    # Map column names to null counts as a dictionary
    total_null_values = {col: count for (col, *_), count in zip(schema_info, result_nulls)}
    print("Total null values: ", total_null_values)

    # Count whitespaces/empty strings in product id columns
    asin_space_only, parent_asin_space_only = con.execute(f"""
        SELECT
        SUM(CASE WHEN TRIM(asin) = '' THEN 1 ELSE 0 END) AS asin_whitespace,
        SUM(CASE WHEN TRIM(parent_asin) = '' THEN 1 ELSE 0 END) AS parent_asin_whitespace
        FROM {table_name}
        """
    ).fetchone()
    print("Sum of whitespace product ids: ", asin_space_only, parent_asin_space_only)

    # Drop rows where both 'asin' and 'parent_asin' is null
    if total_null_values['asin'] > 0 or total_null_values['parent_asin'] > 0:
        con.execute(f"""
            DELETE FROM {table_name}
            WHERE asin IS NULL
            AND parent_asin IS NULL
        """)
        remaining_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print("Remaining rows after dropping product id nulls:", remaining_rows)
    
    # Drop rows where both 'asin' and 'parent_asin' is whitespace/empty string
    if asin_space_only > 0 or parent_asin_space_only > 0:
        con.execute(f"""
            DELETE FROM {table_name}
            WHERE TRIM(asin) = ''
            AND TRIM(parent_asin) = ''
        """)
        remaining_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print("Remaining rows after dropping product id where value is space only:", remaining_rows)
    
    # Filter verified purchases
    con.execute(f"""
        DELETE FROM {table_name}
        WHERE verified_purchase = FALSE
        OR verified_purchase IS NULL
    """)

    # Count the number of rows after filtering verified purchases == 31097566
    num_rows_purchase_true_df = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Number of rows with verified purchases: {num_rows_purchase_true_df}")

    # Read the table into a Polars DataFrame
    df_no_dupes_purchase_true = pl.from_arrow(con.execute(f"SELECT * FROM {table_name}").arrow())

    # Close connection
    con.close()

    # Check the count of data to ensure number of rows matches the number of rows in table created == 31097566
    print(f"Polars DataFrame shape: {df_no_dupes_purchase_true.shape}")
    print(f"Schema: {df_no_dupes_purchase_true.schema}")

    # Clean up text: Convert to lowercase and remove extra spaces
    text_cols_to_clean = ["title", "text"]
    df_cleaned_texts = (df_no_dupes_purchase_true.lazy().with_columns(
        [
            (
                pl.col(texts)
                .str.to_lowercase()
                .str.replace("\t", " ")           # literal replacement
                .str.replace("\n", " ")           # literal replacement
                .str.replace("\r", " ")           # literal replacement
                .str.replace("  ", " ")           # collapse doubles
                .str.replace("  ", " ")           # run twice to remove triples
                #.str.lstrip_chars()                      # remove trailing and leading whitespace
            )
            for texts in text_cols_to_clean
        ])
        .collect(streaming=True) # process operation in batches
    )
    print("Texts columns have been cleaned")

    # Convert milliseconds timestamp to datetime YYYY-MM-DD 09:57:33.520000 with polars from_epoch function
    df_clean_ready_to_use = df_cleaned_texts.with_columns(
        pl.from_epoch("timestamp", time_unit="ms").alias("datetime_of_review")
    )
    print(f"Schema with timestamp added: {df_clean_ready_to_use.schema}")
    print(df_clean_ready_to_use.head())

    return df_clean_ready_to_use

""" EXAMPLE USAGE
parquet_dataset_path = '/data/datafile.parquet'
df_cleaned = transform_dataset(parquet_dataset_path) """
