import pandas as pd
import duckdb


def export_table_to_excel(db_path, table_name, excel_output_path="./data/output"):
    """
    Export a DuckDB table to Excel if it has 1 million rows or fewer.
    Otherwise, prints a message that it's too large.
    
    Parameters:
        db_path (str): Path to the DuckDB database file
        table_name: Name of the table to export
        excel_output_path: Folder for the excel
    """
    con = duckdb.connect(db_path)
    # Count rows in the table
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    if row_count <= 1_000_000:
        # Fetch data to pandas
        df = con.execute(f"SELECT * FROM {table_name}").df()
        excel_file = f"{excel_output_path}{table_name}.xlsx"
        # Close connection
        con.close()
        df.to_excel(excel_file, index=False)
        print(f"Table '{table_name}' exported to Excel: {excel_file} ({row_count} rows)")
    else:
        print(f"Table '{table_name}' has {row_count} rows. Records are more than a million. Cannot be written to Excel.")

# EXAMPLE USAGE
# db_path = 'data/db/sales_database.duckdb'
# table_name = "products_table"
# export_table_to_excel(db_path, table_name)