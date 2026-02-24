
import duckdb

# Connect to the DuckDB database file
con = duckdb.connect(database='D:/data/DataEngineering/Data_Enginerring_Zoomcamp/taxi-pipeline/taxi_pipeline.duckdb', read_only=True)

# Set the schema to ny_taxi_data
con.execute('SET search_path = ny_taxi_data')

# List tables in the schema
print("Tables in 'ny_taxi_data' schema:")
tables = con.execute("SHOW TABLES").fetchall()
for table in tables:
    print(table[0])

# Get the column names from the taxi_trips table
print("Columns in 'taxi_trips' table:")
columns = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'taxi_trips'").fetchall()
for column in columns:
    print(column[0])

# Count rows in the taxi_trips table
print("Row count in 'taxi_trips' table:")
row_count = con.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
print(row_count)
