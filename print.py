import duckdb

conn = duckdb.connect('database/transactions.db')
tables = conn.execute('SHOW TABLES')
first_rows = conn.execute('SELECT * FROM df_transactions LIMIT 4')

sum_price_per_store = conn.execute('SELECT store, SUM(price) AS total_price FROM transactions GROUP BY store')
df_sum = sum_price_per_store.fetch_df()
print(df_sum)

tablesdf = tables.fetch_df()
firstdf = first_rows.fetch_df()
print(tablesdf)
print(firstdf)

conn.close()