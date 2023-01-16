import snowflake.connector
from getTableNames import bhatbhateni_tables
from validate import cnx

database = 'PALASH_BHATBHATENI_DWH'
schema = 'STG'

# Create a cursor object
cursor = cnx.cursor()

# Use the cursor to execute a SQL statement to create a new database
for table in bhatbhateni_tables:
    print("table:",table)
    truncate_sql = f"""
    Truncate table {database}.{schema}.{table}
    """
    cursor.execute(truncate_sql)
    insert_sql = f"""
        CREATE or replace TABLE {database}.{schema}.{table} AS SELECT * FROM BHATBHATENI.TRANSACTIONS.{table};
    """
    cursor.execute(insert_sql)

# Close the cursor and the connection
cursor.close()
