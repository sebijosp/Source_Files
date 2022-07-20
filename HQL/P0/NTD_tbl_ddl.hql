CREATE TABLE HEM.NTD (
   MAC             STRING,
   PHUB            STRING,
   CMTSIP          STRING,
   NODE            STRING,
   SMT             STRING,
   STREAM          STRING,
   FWDSEG          STRING,
   CLAMSHELL       STRING,
   STREET_NUMBER   DECIMAL,
   STREET_NAME     STRING,
   STREET_TYPE     STRING,
   STREET_COMPASS  STRING,
   APT_NUMBER      STRING,
   POSTAL_CODE     STRING,
   CITY_NAME       STRING,
   PROVINCE        STRING,
   PRODUCT_CODE    STRING,
   HDP_INSERT_TS   TIMESTAMP,
   HDP_UPDATE_TS   TIMESTAMP,
   HDP_FILE_NAME    STRING,
   JOB_EXECUTION_ID STRING
) STORED AS ORC;
