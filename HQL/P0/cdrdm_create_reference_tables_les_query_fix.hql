CREATE EXTERNAL TABLE cdrdm.dim_measured_object(
measured_object_id DECIMAL(9,0),
network_element_id DECIMAL(5,0),
map_id DECIMAL(5,0),
measured_object_type CHAR(1),
measured_object VARCHAR(50),
measure_object_type_desc VARCHAR(50),
inservice CHAR(1),
comissioned STRING,
decomissioned STRING,
measured_object_full_name VARCHAR(50),
measured_object_prefix CHAR(10),
measured_object_suffix CHAR(10),
latitude DECIMAL(38,13),
longitude DECIMAL(38,13),
alias_1 CHAR(50),
postalcode CHAR(7),
monum CHAR(14),
monum_dec CHAR(15),
load_time_stamp STRING,
First_Cell_ID_Extension CHAR(4))
PARTITIONED BY (sqoop_ext_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
LOCATION '/userapps/cdrdm/landing/tdprod/CDR_PROD_MART/dim_measured_object';

CREATE EXTERNAL TABLE cdrdm.dim_network_element(
network_element_id DECIMAL(5,0),
network_element_type CHAR(1),
network_element VARCHAR(50),
network_element_type_desc VARCHAR(50),
level_number DECIMAL(3,0),
left_range DECIMAL(38,0),
right_range DECIMAL(38,0),
time_off_set DECIMAL(38,0),
map_id DECIMAL(5,0),
source_type CHAR(1),
ip VARCHAR(32),
nss7 VARCHAR(32),
alias_1 VARCHAR(50),
load_time_stamp STRING)
PARTITIONED BY (sqoop_ext_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
LOCATION '/userapps/cdrdm/landing/tdprod/CDR_PROD_MART/dim_network_element';

MSCK REPAIR TABLE cdrdm.dim_measured_object;
MSCK REPAIR TABLE cdrdm.dim_network_element;

CREATE VIEW IF NOT EXISTS cdrdm.v_measured_object AS
SELECT dim_measured_object.measured_object_id,
            CAST(dim_measured_object.monum AS VARCHAR(50)) AS natural_key,
              CAST(dim_measured_object.monum_dec AS VARCHAR(50)) AS description,
             dim_measured_object.measured_object_type,
               first_cell_id_extension,

                 CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' 
                       THEN CONCAT(SUBSTR(dim_measured_object.monum,1,6),
                               TRIM(REGEXP_EXTRACT(TRIM(SUBSTR(dim_measured_object.monum,7,4)), '(0*)(.*)', 2)),
                               TRIM(REGEXP_EXTRACT(TRIM(SUBSTR(dim_measured_object.monum,11,4)), '(0*)(.*)', 2))) 
                         ELSE  CONCAT(SUBSTR(dim_measured_object.monum,1,6),
                               TRIM(REGEXP_EXTRACT(TRIM(SUBSTR(dim_measured_object.monum,7,4)), '(0*)(.*)', 2)),
                                TRIM(REGEXP_EXTRACT(TRIM(first_cell_id_extension), '(0*)(.*)', 2)), 
                                SUBSTR(TRIM(dim_measured_object.monum),11,4))
                   END AS LTE_HSPA_CellSite,
                   
                   CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' 
                       THEN TRIM(dim_measured_object.monum)
                         ELSE CONCAT(SUBSTR(TRIM(dim_measured_object.monum),1,10),
                         TRIM(first_cell_id_extension),SUBSTR(TRIM(dim_measured_object.monum),11,4))
                   END AS LTE_HSPA_CellSite_Orig

FROM cdrdm.dim_measured_object dim_measured_object
WHERE dim_measured_object.measured_object_type = 'C'
                 
UNION ALL
                      
SELECT dim_measured_object.measured_object_id,
                    dim_measured_object.measured_object AS natural_key,
                    dim_measured_object.measured_object AS description,
                    dim_measured_object.measured_object_type,
                    first_cell_id_extension,
                    CAST(NULL AS VARCHAR(20))  LTE_HSPA_CellSite,
                    CAST(NULL AS VARCHAR(20))  LTE_HSPA_CellSite_Orig
FROM cdrdm.dim_measured_object dim_measured_object
WHERE dim_measured_object.measured_object_type = 'r'
OR  dim_measured_object.measured_object_type = 'R';