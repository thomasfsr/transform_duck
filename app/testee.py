import pandas as pd
from db_tool import DuckdbTransform

dbt = DuckdbTransform(db_name = 'transaction')
conn = dbt.connect_to_db()
query = 'SELECT * FROM df_transactions'
df = pd.read_sql_query(query, conn)
conn.close()

memory_usage_series = df.memory_usage(deep=True)
memory_usage_mb_series = memory_usage_series / (1024 * 1024)  # Convert bytes to megabytes

print("Memory usage per column (in MB):")
print(memory_usage_mb_series)