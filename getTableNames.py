import snowflake.connector
from validate import cnx

database = 'BHATBHATENI'
schema = 'Transactions'

cursor = cnx.cursor()

# Execute the SHOW TABLES command
cursor.execute(f"SHOW TABLES IN {database}.{schema}")

# variable to store all Table names from source Schema
source_tables = []

# Loop through the results and get the table names
for result in cursor:
    column_names = [column[0] for column in cursor.description]
    table_attributes = {name: value for name, value in zip(column_names, result)}
    source_tables.append(table_attributes['name'])

bhatbhateni_tables=source_tables

# Close the cursor and the connection
cursor.close()