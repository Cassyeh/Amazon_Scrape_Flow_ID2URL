import duckdb
from .create_duckdb_table import connect_to_duckdb, check_table_exists, preview_duckdb_table
from .web_scraping import web_scrape_search


def add_empty_columns(db_path, table_name, extra_columns):
    """
    Adds empty (NULL) columns to an existing DuckDB table.
    """
    con = connect_to_duckdb(db_path)
    # Check if table exists
    if (check_table_exists(con, table_name)):
        for col in extra_columns:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR;")
        print(f"Added empty columns {extra_columns} to table '{table_name}'.")
        preview_duckdb_table(con, table_name)

# EXAMPLE USAGE
# add_empty_columns("./data/output/sales_db", "top_products", ["source", "region", "category"])

def insert_product_url_from_web(db_path, input_table, output_table, delay=3.5):
    """
    Process ProductIDs directly from a DuckDB table and write results to an output table.
    """
    con = connect_to_duckdb(db_path)
    # Create output table with additional columns
    con.execute(f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT Product_ID, '' AS Product_Name, '' AS URL
        FROM {input_table}
    """)

    # Fetch all rows
    rows = con.execute(f"SELECT Product_ID FROM {output_table}").fetchall()
    product_ids = [row[0] for row in rows]

    for row in product_ids:
        product_id = row
        search_results = web_scrape_search(product_id)

        if search_results:
            title = search_results[0]["title"]
            url = search_results[0]["url"]
        else:
            title = "No result"
            url = ""

        # Update row directly in DuckDB
        con.execute(f"""
            UPDATE {output_table}
            SET Product_Name = ?, URL = ?
            WHERE Product_ID = ?
        """, [title, url, product_id])

    print(f"Processing complete. Results written to '{output_table}'.")

    # Close connection
    con.close()

""" EXAMPLE USAGE
input_table = "input_asins"   # table with asin
output_table = "product_url_table"
insert_product_url_from_web("./data/output/sales_db", input_table, output_table) """
