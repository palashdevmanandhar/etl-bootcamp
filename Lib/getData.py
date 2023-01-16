import sys

root_directory= '/Users\palash.manandhar\PycharmProjects\etlPythonProject'
sys.path.append(root_directory)

import validate

cnx=validate.cnx
cursor=cnx.cursor()


def getAllData (database,schema,table):
    cursor.execute(f"use database {database}")
    cursor.execute(f"use schema {schema};")
    getStagingTableData = f"""
                SELECT *
                FROM {table}
            """
    result = cursor.execute(getStagingTableData);
    rows = result.fetchall()
    column_names = [i[0] for i in cursor.description]
    sourceData = []
    for row in rows:
        sourceData.append({column_names[i]: row[i] for i in range(len(column_names))})
    return sourceData