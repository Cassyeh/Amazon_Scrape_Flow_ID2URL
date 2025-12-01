import duckdb


def connect_to_duckdb(db_path: str):
    """Method that connects to duckdb"""
    return duckdb.connect(db_path)

def check_table_exists(con, table_name):
    """
    Checks if a DuckDB table exists.
    If it exists:
        - returns True
    If not:
        - returns False (caller can continue creating table)
    """

    # Check if the table exists
    table_exists = con.execute(
        """
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = ?
        """,
        [table_name.lower()]
    ).fetchone()

    if table_exists:
        return True   # signifies table already existed

    return False  # table does not exist, caller can proceed to create it

def build_filter_clause(filter_column=None, filter_operator=None, filter_value=None):
    """
    Build a WHERE clause and filter flag for DuckDB queries.

    Parameters:
        filter_column (str): Column to filter by (optional)
        filter_operator (str): SQL operator, e.g. '<', '>', '=', '<=' (optional)
        filter_value: Value for filter (optional)

    Returns:
        filter_clause (str): SQL WHERE clause or empty string.
        filter_flag (str): "TRUE" if filtering is applied, otherwise "FALSE".
    """
    # No filter provided → return defaults
    if not (filter_column and filter_operator and filter_value is not None):
        return "", "FALSE"

    # Create the SQL WHERE clause
    filter_clause = f"WHERE {filter_column} {filter_operator} {filter_value}"
    return filter_clause, "TRUE"

def build_limit_clause(limit):
    """
    Returns a SQL LIMIT clause or an empty string if limit is None.
    """
    if limit is None:
        return ""          # fetch all
    return f"LIMIT {limit}"   # fetch n

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

def create_grouped_table( 
    db_path,
    table_name,
    source_table="amazon_reviews",
    group_column="asin",
    new_column="Product_ID",
    limit=10000,
    filter_column=None,
    filter_operator=None,
    filter_value=None
):
    """
    Create a DuckDB table containing column counts with optional filtering.
    
    Parameters:
        db_path (str): Path to the DuckDB database file
        table_name: Output table name
        source_table: Input reviews table
        group_column: column to be GROUP BY
        limit: Number of records to return
        filter_column: Column to filter by (optional)
        filter_operator: SQL operator, e.g. '<', '>', '=', '<=' (optional)
        filter_value: Value for filter (optional)
    """

    con = connect_to_duckdb(db_path)
    print("Connection successful")

    # Check if table exists
    # table_exists = check_table_exists(con, table_name)
    # if table_exists:
    #     preview_duckdb_table(con, table_name)
    #     return table_name
    
    # Determine if a filter should be applied
    filter_clause, filter_flag = build_filter_clause(filter_column, filter_operator, filter_value)

    # Build limit clause
    limit_clause = build_limit_clause(limit)

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        WITH filtered AS (
            SELECT *,
                {filter_flag}::BOOLEAN AS filter_applied
            FROM {source_table}
            {filter_clause}
        ),
        grouped AS (
            SELECT
                {group_column} AS {new_column},
                COUNT(*) AS count,
                MAX(filter_applied) AS filter_applied
            FROM filtered
            GROUP BY {group_column}
        )
        SELECT {new_column}, count
        FROM grouped
        ORDER BY count DESC
        {limit_clause};
    """)
    # con.execute(f"ALTER TABLE {table_name} DROP COLUMN filter_applied;")
    # Close connection
    con.close()
    print(f"Table '{table_name}' created successfully (limit={limit}).")
    return table_name

# EXAMPLE USAGE
# create_grouped_table(con, "top_products", group_column="asin", limit=10000)
