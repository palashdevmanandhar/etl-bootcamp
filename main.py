import snowflake.connector
import csv

# Replace these values with your own Snowflake account and login details
account = 'jw73204.ap-south-1.aws'
user = 'Palash'
password = 'Demo@123'
database = 'BHATBHATENI'
schema = 'TRANSACTIONS'
warehouse='COMPUTE_WH'

# Connect to Snowflake
cnx = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    database=database,
    schema=schema,
)

# Create a cursor
cursor = cnx.cursor()

# Execute a SELECT statement with a JOIN clause to retrieve the data from the tables
cursor.execute("""
SELECT * from Sales;
""")

# Fetch the results
results = cursor.fetchall()

# Close the connection
cnx.close()

# Open a file for writing
with open('hierarchy.csv', 'w', newline='') as csvfile:
    # Create a CSV writer
    writer = csv.writer(csvfile)

    # Write the column names
    writer.writerow(['STORE_ID', 'COUNTRY_DESC', 'REGION_DESC',  'STORE_DESC'])

    # Write the rows
    for row in results:
        writer.writerow(row)