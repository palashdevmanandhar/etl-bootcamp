import snowflake.connector

account = 'jw73204.ap-south-1.aws'
user = 'Palash'
password = 'Demo@123'
warehouse='COMPUTE_WH'
target_database = 'PALASH_BHATBHATENI_DWH'
staging_schema = 'STG'
temp_schema = 'TMP'
target_schema = 'TGT'

# Connect to Snowflake
cnx = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    # database=database,
    # schema=schema,
)

