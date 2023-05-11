import duckdb
import pyarrow.parquet as pq

# Connect to an in-memory DuckDB database
con = duckdb.connect(database=':memory:')

# Install the TPC-H extension and load the data
con.execute("INSTALL tpch; LOAD tpch")

# Generate data for scale factor 10
# If you go to 100 make sure to have ~500GB of memory available
con.execute("CALL dbgen(sf=10)")

# Show the list of tables in the database
tables = con.execute("show tables").fetchall()
print(tables)

# Export each table as a Parquet file
for table_name in tables:
    table_name = table_name[0]
    result = con.query(f"SELECT * FROM {table_name}")
    print(f"Writing {table_name}")
    pq.write_table(result.to_arrow_table(), f"{table_name}.parquet")