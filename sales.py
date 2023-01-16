import snowflake.connector

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

# Fetch the results of the SELECT statement as a Pandas dataframe
df = cursor.fetch_pandas_all()

# Export the dataframe to a CSV file
df.to_csv("sales.csv", index=False)

# Close the cursor and connection
cursor.close()
cnx.close()