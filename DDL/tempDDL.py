import sys
import os

current_directory = os.getcwd()
root_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(root_directory)

import validate

cnx = validate.cnx
target_database = validate.target_database
temp_schema = validate.temp_schema

print(temp_schema,target_database)

cursor = cnx.cursor();

cursor.execute(f"use database {target_database}")
cursor.execute(f"use schema {temp_schema};")

cursor.execute("""
CREATE OR REPLACE TABLE DWH_D_CNTRY
(
  CNTRY_KY NUMBER AUTOINCREMENT,
  CNTRY_ID NUMBER NOT NULL,
  CNTRY_DESC VARCHAR(50),
  OPEN_CLOSE_FLG VARCHAR(1),
  RCD_INS_TS TIMESTAMP_NTZ,
  RCD_UPD_TS TIMESTAMP_NTZ,
  CONSTRAINT CNTRY_PK PRIMARY key (CNTRY_KY)
);""")

cursor.execute("""
CREATE OR REPLACE TABLE DWH_D_REGION
(
  RGN_KY NUMBER AUTOINCREMENT,
  RGN_ID NUMBER NOT NULL,
  CNTRY_KY NUMBER,
  RGN_DESC VARCHAR(50),
  OPEN_CLOSE_FLG VARCHAR(1),
  RCD_INS_TS TIMESTAMP_NTZ,
  RCD_UPD_TS TIMESTAMP_NTZ,
  CONSTRAINT RGN_PK PRIMARY key (RGN_KY),
  CONSTRAINT CNTRY_FK FOREIGN key (CNTRY_KY) REFERENCES DWH_D_CNTRY(CNTRY_KY)
);
""")

cursor.execute("""
CREATE OR REPLACE TABLE DWH_D_STORE
(
  STR_KY NUMBER AUTOINCREMENT,
  STR_ID NUMBER NOT NULL,
  RGN_KY NUMBER,
  CNTRY_KY NUMBER,
  STR_DESC VARCHAR(50),
  OPEN_CLOSE_FLG VARCHAR(1),
  RCD_INS_TS TIMESTAMP_NTZ,
  RCD_UPD_TS TIMESTAMP_NTZ,
  CONSTRAINT LOCN_PK PRIMARY KEY (STR_KY),
  CONSTRAINT RGN_FK FOREIGN KEY (RGN_KY) REFERENCES DWH_D_REGION(RGN_KY),
  CONSTRAINT CNTRY_FK FOREIGN KEY (CNTRY_KY) REFERENCES DWH_D_CNTRY(CNTRY_KY)
);
""")
