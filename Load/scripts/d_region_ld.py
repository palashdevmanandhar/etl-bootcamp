import sys

root_directory = '/Users\palash.manandhar\PycharmProjects\etlPythonProject'
sys.path.append(root_directory)
from validate import cnx, target_database, staging_schema, temp_schema,target_schema

lib_directory = '/Users\palash.manandhar\PycharmProjects\etlPythonProject\Lib'
sys.path.append(lib_directory)

from getData import getAllData

cursor = cnx.cursor()

staging_table = 'REGION'
temp_table = 'DWH_D_REGION'
target_table='DWH_D_REGION'

DWH_D_CNTRY_COLUMNS_MAPPING = [{'source': 'ID', 'target': 'RGN_ID', 's_table': 'staging_schema.COUNTRY'},
                               {'source': 'REGION_DESC', 'target': 'RGN_DESC', 's_table': 'staging_schema.COUNTRY'},
                               {'source': 'COUNTRY_ID', 'target': 'CNTRY_KY', 's_table': 'target_schema.DWH_D_CNTRY'},
                               ]

def insertDataToTable(data, target_table):
    print('inserting into table', data, data['ID'], data['REGION_DESC'],data['COUNTRY_ID'], target_table)
    insert_sql = f"""
        INSERT INTO {target_table} (RGN_ID,CNTRY_KY, RGN_DESC, OPEN_CLOSE_FLG,RCD_INS_TS,RCD_UPD_TS)
        SELECT {data['ID']}, DWH_D_CNTRY.CNTRY_KY, '{data['REGION_DESC']}', 'Y', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        FROM DWH_D_CNTRY
        WHERE DWH_D_CNTRY.CNTRY_ID = {data['COUNTRY_ID']}   
        """
    cursor.execute(f"use database {target_database}")
    cursor.execute(f"use schema {temp_schema};")
    cursor.execute(insert_sql)


def checkForMajorChanges(targetItem, target_table):
    print("major chnge incoming", targetItem, target_table)
    update_sql = f"""
                    UPDATE {target_table}
                    SET OPEN_CLOSE_FLG = 'N',RCD_UPD_TS=CURRENT_TIMESTAMP()
                    WHERE RGN_KY = {targetItem['RGN_KY']};
                    """
    cursor.execute(f"use database {target_database}")
    cursor.execute(f"use schema {temp_schema};")
    cursor.execute(update_sql)


def checkForMinorChanges(source_data, target_data, target_table):
    # print(source_data['COUNTRY_DESC'],target_data['CNTRY_DESC'])
    if source_data['COUNTRY_DESC'] != target_data['CNTRY_DESC']:
        print('minor changes found for', source_data, target_data)
        insert_sql = f"""
                UPDATE {target_table}
                SET CNTRY_DESC='{source_data['COUNTRY_DESC']}'
                WHERE CNTRY_ID={target_data['CNTRY_ID']}
                """
        cursor.execute(f"use database {target_database}")
        cursor.execute(f"use schema {temp_schema};")
        cursor.execute(insert_sql)
    else:
        print('no minor changes for ', source_data, target_data)


def checkMatchingData(source_data, target_data, target_table):
    target_data_primary_keys = []
    source_data_primary_keys = []

    # get primary keys for all target table
    for item in target_data:
        target_data_primary_keys.append(item['RGN_ID'])

    # get primary keys for all source table
    for item in source_data:
        source_data_primary_keys.append(item['ID'])

    # check if all sourcedata are present in the target table
    for index, sourceItem in enumerate(source_data):
        if sourceItem['Id'.upper()] in target_data_primary_keys:
            # checkForMinorChanges(source_data[index],target_data[index],target_table)
            # handel minor changes if any
            print('one data already present in target')
        else:
            # insert source data which are not present in the target table
            insertDataToTable(source_data[index], target_table)

    # check if any sourcedata is missing from the target table
    for index, targetItem in enumerate(target_data):
        if targetItem['RGN_ID'.upper()] not in source_data_primary_keys:
            checkForMajorChanges(targetItem, target_table)


def load_temp_table():
    all_staging_data = getAllData(target_database, staging_schema, staging_table)
    all_temp_data = getAllData(target_database, temp_schema, temp_table)
    print("all temp data",all_temp_data)
    checkMatchingData(all_staging_data, all_temp_data, temp_table)


load_temp_table()

def load_target_table():
    truncate_sql=f"""
    Truncate table {target_table}
    """
    load_sql=f"""
    INSERT INTO {target_table}
    SELECT * FROM {temp_schema}.{temp_table};
    """
    cursor.execute(f"use database {target_database}")
    cursor.execute(f"use schema {target_schema};")
    cursor.execute(truncate_sql)
    cursor.execute(load_sql)

load_target_table()