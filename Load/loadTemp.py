import sys
import os

current_directory = os.getcwd()
root_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(root_directory)

import validate

cnx = validate.cnx
target_database = validate.target_database
staging_schema = validate.staging_schema
temp_schema = validate.temp_schema
target_schema = validate.target_schema

cursor = cnx.cursor();

mappingList=[{'Source':'STORE','Target':'DWH_D_STORE','Type':'Dimension'},
             {'Source':'REGION','Target':'DWH_D_REGION','Type':'Dimension'},
             {'Source':'COUNTRY','Target':'DWH_D_CNTRY','Type':'Dimension'}]


DWH_D_CNTRY_COLUMNS_MAPPING=[{'source':'ID','target':'CNTRY_ID','s_table':'COUNTRY'} ,
                             {'source': 'COUNTRY_DESC', 'target': 'CNTRY_DESC', 's_table':'COUNTRY'},
                             ]

DWH_D_REGION_COLUMNS_MAPPING=[{'source':'ID','target':'RGN_ID','s_table':'REGION'} ,
                             {'source': 'CNTRY_KY', 'target': 'CNTRY_KY','s_table':'COUNTRY'},
                             {'source': 'REGION_DESC', 'target': 'RGN_DESC','s_table':'REGION'},
                             ]

DWH_D_STORE_COLUMNS_MAPPING=[{'source':'ID','target':'STR_ID','s_table':'STORE'} ,
                             {'source': 'CNTRY_KY', 'target': 'CNTRY_KY','s_table':'COUNTRY'},
                             {'source': 'RGN_KY', 'target': 'RGN_KY','s_table':'REGION'},
                             {'source': 'REGION_DESC', 'target': 'STR_DESC','s_table':'STORE'},
                             ]




def getSourceData (sourceTable):
    cursor.execute(f"use database {target_database}")
    cursor.execute(f"use schema {staging_schema};")
    getStagingTableData = f"""
                SELECT *
                FROM {sourceTable}
            """
    sourceResult = cursor.execute(getStagingTableData);
    rows = sourceResult.fetchall()
    column_names = [i[0] for i in cursor.description]
    sourceData = []
    for row in rows:
        sourceData.append({column_names[i]: row[i] for i in range(len(column_names))})
    return sourceData

def getTargetData (targetTable):
    cursor.execute(f"use database {target_database}")
    cursor.execute(f"use schema {temp_schema};")
    getStagingTableData = f"""
                SELECT *
                FROM {targetTable}
            """
    sourceResult = cursor.execute(getStagingTableData);
    rows = sourceResult.fetchall()
    column_names = [i[0] for i in cursor.description]
    print(f"target columns",column_names)
    sourceData = []
    for row in rows:
        sourceData.append({column_names[i]: row[i] for i in range(len(column_names))})
    return sourceData

def insertDataToTable (data,targetTable):
    print('inserting into table')


def checkMatchingData (sourceData,targetData,targetTable):
    targetDataPrimaryKeys = []
    for item in targetData:
        targetDataPrimaryKeys.append(item['RGN_ID'])
    for sourceItem in sourceData:
            if sourceItem['Id'.upper()] in targetDataPrimaryKeys:
                print('record is present')
            else:
                # print('record not present')
                insertDataToTable(sourceData,targetTable)



def loadTemp() :
    for tablePair in mappingList:
        sourceTable=tablePair['Source']
        targetTable=tablePair['Target']
        sourceData=getSourceData(sourceTable)
        targetData=getTargetData(targetTable)
        checkMatchingData(sourceData,targetData,targetTable)

loadTemp()


