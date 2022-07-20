CREATE EXTERNAL TABLE cdrdm.dim_rhpc_call_type(
call_type_cd INT,
description CHAR(100),
crtn_tmst STRING)
PARTITIONED BY (sqoop_ext_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
LOCATION '/userapps/cdrdm/landing/tdprod/CDR_PROD_MART/dim_rhpc_call_type';

MSCK REPAIR TABLE cdrdm.dim_rhpc_call_type;