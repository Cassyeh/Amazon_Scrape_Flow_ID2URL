import sqlite3
import pandas as pd

def build_create_table_query(df, table_name: str, id_primary_key: bool = True):
    """
    Build a CREATE TABLE SQL query dynamically from a pandas DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to map to a table
        table_name (str): Name of the table to create
        id_primary_key (bool): Whether to add an auto-increment 'id' primary key column (default True)
    
    Returns:
        str: CREATE TABLE SQL query
    """
    # Map pandas dtypes to SQLite types
    dtype_map = {
        "int64": "INTEGER",
        "float64": "REAL",
        "object": "TEXT",
        "bool": "INTEGER",          # store as 0/1 True → 1 and False → 0
        "datetime64[ns]": "TEXT"    # store datetime as ISO string
    }
    
    db_columns = []
    for col in df.columns:
        col_dtype = str(df[col].dtype) # get pandas dtype as string
        print(col, " ", col_dtype)
        sqlite_type = dtype_map.get(col_dtype, "TEXT") # default to TEXT if unknown
        db_columns.append(f'"{col}" {sqlite_type}')
    
    columns_sql = ", ".join(db_columns)
    
    if id_primary_key:
        columns_sql = "id INTEGER PRIMARY KEY AUTOINCREMENT, " + columns_sql
    
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});'
    print(create_table_sql)
    return create_table_sql

""" EXAMPLE USAGE
# Build CREATE TABLE query
table_name = 'amazon_reviews'
create_sql_query = build_create_table_query(df, table_name) """


def create_table_in_sqlite(db_path: str, create_table_sql: str, table_name):
    """
    Connect to a SQLite database and execute a CREATE TABLE SQL query.
    
    Args:
        db_path (str): Path to the SQLite database file
        create_table_sql (str): CREATE TABLE SQL statement
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    # Commit the transaction to save changes
    conn.commit()

    # Verify that table was created
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
    columns = [description[0] for description in cursor.description]
    print("Columns in table:", columns)
    
    conn.close()
    print(f"Table created or verified in database: {db_path}")

""" EXAMPLE USAGE
# Execute the query in SQLite
# Path to the SQLite database file
db_path = 'data/db/sales_database.db'
table_name = "amazon_reviews"
create_sql_query = build_create_table_query(df, table_name)
create_table_in_sqlite(db_path, create_sql_query, table_name) """