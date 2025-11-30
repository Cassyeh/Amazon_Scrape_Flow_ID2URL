import duckdb
import polars as pl


def connect_to_duckdb(db_path: str):
    """Method that connects to duckdb"""
    return duckdb.connect(db_path)

def validate_table(con, table_name: str, parquet_df_path):
    """Function that validates that the table was loaded completely"""
    num_df_rows = con.execute(
    "SELECT COUNT(*) FROM parquet_scan(?)", 
    [parquet_df_path]).fetchone()[0]
    table_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    # Optional verbose/printing
    print(f"Parquet DF rows:  {num_df_rows}")
    print(f"DuckDB table rows: {table_rows}")

    if num_df_rows == table_rows:
        print("✔ Data loaded successfully.")
        return True
    else:
        print("✘ Row count mismatch. Data not loaded correctly.")
        return False

def preview_duckdb_table(con, table_name: str, n: int = 5):
    """
    Prints the first n rows of a DuckDB table.

    Args:
        con: DuckDB connection object
        table_name (str): Name of the table to preview
        n (int): Number of rows to display (default 5)
    """
    query = f"SELECT * FROM {table_name} LIMIT {n}"
    preview_table = con.execute(query).fetchdf()
    
    print(f"--- Preview of table '{table_name}' (first {n} rows) ---")
    print(preview_table)
    print("-" * 50)

def load_parquet_to_duckdb(db_path: str, table_name, parquet_df_path):
    """
    Function that writes a parquet file to a DuckDB table
    
    Args:
        db_path (str): Path to the DuckDB database file
        table_name (str): table in db to be created
        parquet_df_path (str): file where the df is stored as parquet
    """
    # Create a connection
    con = connect_to_duckdb(db_path)
    print("Connection successful")

    # Check if table exists with data
    table_exists = con.execute("""SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = ?""",
        [table_name.lower()]
    ).fetchone()

    if table_exists:
        print(f"Table: {table_name} exists in database: {db_path}")
        # Validate table
        validate_table(con, table_name, parquet_df_path)
        preview_duckdb_table(con, table_name)
        # Close connection
        con.close()
        return
    
    # Table does NOT exist - create it
    print(f"Table {table_name} does NOT exist. Creating....")

    # Write directly from Parquet path into a DuckDB table
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM '{parquet_df_path}'
        """)
    print(f"Table {table_name} created")
    

    # Verify that table was created
    num_table_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    # Validate table
    table_valid = validate_table(con, table_name, parquet_df_path)
    preview_duckdb_table(con, table_name)
    
    # Close connection
    con.close()
    if table_valid:
        print(f"Table created and verified in database: {db_path}")
    else:
        print(f"✘ Table '{table_name}' created but validation failed.")

""" EXAMPLE USAGE
parquet_path = polars_to_parquet(df_cleaned, "./data/output/df_cleaned.parquet")
# Path to the DuckDb database file
db_path = 'data/db/sales_database.duckdb'
table_name = "amazon_reviews"
load_parquet_to_duckdb(db_path, table_name, parquet_path) """