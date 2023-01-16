import snowflake.connector

account = 'jw73204.ap-south-1.aws'
user = 'Palash'
password = 'Demo@123'
warehouse='COMPUTE_WH'


# Connect to Snowflake
cnx = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
)

# Create a cursor object
cursor = cnx.cursor()

# Use the cursor to execute a SQL statement to create a new database
cursor.execute("CREATE DATABASE PALASH_BHATBHATENI_DWH")

# Close the cursor and the connection
cursor.close()
cnx.close()