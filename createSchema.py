import snowflake.connector

account = 'jw73204.ap-south-1.aws'
user = 'Palash'
password = 'Demo@123'
warehouse='COMPUTE_WH'
database = 'PALASH_BHATBHATENI_DWH'


# Connect to Snowflake
cnx = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    database=database,
)

# Create a cursor object
cursor = cnx.cursor()

# A list of schema names to be created
schemas=['STG', 'TMP' , 'TGT']
# cursor.execute("CREATE SCHEMA my_schema")
for schema in schemas:
    cursor.execute(f"CREATE SCHEMA {schema}")

# Close the cursor and the connection
cursor.close()
cnx.close()