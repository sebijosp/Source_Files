SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

SET last_proc_data_dt;
SET yesterday_dt;

DROP TABLE IF EXISTS ext_cdrdm.dim_cell_site_info_lte_temp1;
DROP TABLE IF EXISTS ext_cdrdm.dim_cell_site_info_umts_temp1;
DROP TABLE IF EXISTS ext_cdrdm.dim_cell_site_info_gsm_temp1;

MSCK REPAIR TABLE ext_cdrdm.dim_cell_site_info_lte;

CREATE TABLE IF NOT EXISTS ext_cdrdm.dim_cell_site_info_lte_temp1 AS
SELECT
*,
LOCATE('-', trim(cgi)) as first_occurence,
LOCATE('-', trim(cgi), cast(LOCATE('-',trim(cgi)) as int)+1) as second_occurrence,
length(cgi) - LOCATE('-', reverse(trim(cgi))) + 1 as last_occurrence
FROM ext_cdrdm.dim_cell_site_info_lte WHERE (file_date > ${hiveconf:last_proc_data_dt} AND file_date <= ${hiveconf:yesterday_dt});

INSERT INTO TABLE cdrdm.dim_cell_site_info
SELECT
trim(site),
trim(cell),
trim(nodeb) as ENODEB,
trim(cgi),
CONCAT(SUBSTR(trim(cgi),1,LOCATE('-',trim(cgi))-1),CONCAT(SUBSTR(trim(cgi),LOCATE('-',trim(cgi))+1,(LOCATE('-',trim(cgi),second_occurrence) - LOCATE('-',trim(cgi)))-1),CONCAT(CONV(SUBSTR(trim(cgi),LOCATE('-',trim(cgi),second_occurrence - 1)+1,(LOCATE('-',trim(cgi),second_occurrence + 1) - LOCATE('-',trim(cgi),second_occurrence -1))-1),10,16),CONV(SUBSTR(trim(cgi),LOCATE('-',trim(cgi),last_occurrence -1)+1),10,16)))) as CGI_HEX,
trim(site_name),
trim(original_i_s_date) as ORIGINAL_I,
trim(antenna_type) as ANTENNA_TY,
trim(azimuth),
trim(beamwidth),
trim(x),
trim(y),
trim(longitude),
trim(latitude),
trim(address),
trim(city),
trim(province),
NULL, --ARFCNDL
NULL, --BCCHNO
trim(locationcode) as LOCATIONCO,
NULL, --BSIC
NULL, --PRIMARYSCR
'LTE', --FILE_TYPE
from_unixtime(unix_timestamp()), --INSERT_TIMESTAMP
file_date
FROM ext_cdrdm.dim_cell_site_info_lte_temp1;

MSCK REPAIR TABLE ext_cdrdm.dim_cell_site_info_umts;

CREATE TABLE IF NOT EXISTS ext_cdrdm.dim_cell_site_info_umts_temp1 AS
SELECT
*,
LOCATE('-', trim(cgi)) as first_occurence,
LOCATE('-', trim(cgi), cast(LOCATE('-',trim(cgi)) as int)+1) as second_occurrence,
length(cgi) - LOCATE('-', reverse(trim(cgi))) + 1 as last_occurrence
FROM ext_cdrdm.dim_cell_site_info_umts WHERE (file_date > ${hiveconf:last_proc_data_dt} AND file_date <= ${hiveconf:yesterday_dt});

INSERT INTO TABLE cdrdm.dim_cell_site_info
SELECT
trim(site),
trim(cell),
NULL, --ENODEB,
trim(cgi),
CONCAT(SUBSTR(trim(cgi),1,LOCATE('-',trim(cgi))-1),CONCAT(SUBSTR(trim(cgi),LOCATE('-',trim(cgi))+1,(LOCATE('-',trim(cgi),second_occurrence) - LOCATE('-',trim(cgi)))-1),CONCAT(CONV(SUBSTR(trim(cgi),LOCATE('-',trim(cgi),second_occurrence - 1)+1,(LOCATE('-',trim(cgi),second_occurrence + 1) - LOCATE('-',trim(cgi),second_occurrence -1))-1),10,16),CONV(SUBSTR(trim(cgi),LOCATE('-',trim(cgi),last_occurrence -1)+1),10,16)))) as CGI_HEX,
trim(site_name),
trim(original_i_s_date) as ORIGINAL_I,
trim(antenna_type) as ANTENNA_TY,
trim(azimuth),
trim(beamwidth),
trim(x),
trim(y),
trim(longitude),
trim(latitude),
trim(address),
trim(city),
trim(province),
trim(arfcndl),
NULL, --BCCHNO
trim(locationcode) as LOCATIONCO,
NULL, --BSIC
trim(primaryscramblingcode) as PRIMARYSCR,
'UMTS', --FILE_TYPE
from_unixtime(unix_timestamp()), --INSERT_TIMESTAMP
file_date
FROM ext_cdrdm.dim_cell_site_info_umts_temp1;

MSCK REPAIR TABLE ext_cdrdm.dim_cell_site_info_gsm;

CREATE TABLE IF NOT EXISTS ext_cdrdm.dim_cell_site_info_gsm_temp1 AS
SELECT
*,
LOCATE('-', trim(cgi)) as first_occurence,
LOCATE('-', trim(cgi), cast(LOCATE('-',trim(cgi)) as int)+1) as second_occurrence,
length(cgi) - LOCATE('-', reverse(trim(cgi))) + 1 as last_occurrence
FROM ext_cdrdm.dim_cell_site_info_gsm WHERE (file_date > ${hiveconf:last_proc_data_dt} AND file_date <= ${hiveconf:yesterday_dt});

INSERT INTO TABLE cdrdm.dim_cell_site_info
SELECT
trim(site),
trim(cell),
NULL, --ENODEB
trim(cgi),
CONCAT(SUBSTR(trim(cgi),1,LOCATE('-',trim(cgi))-1),CONCAT(SUBSTR(trim(cgi),LOCATE('-',trim(cgi))+1,(LOCATE('-',trim(cgi),second_occurrence) - LOCATE('-',trim(cgi)))-1),CONCAT(CONV(SUBSTR(trim(cgi),LOCATE('-',trim(cgi),second_occurrence - 1)+1,(LOCATE('-',trim(cgi),second_occurrence + 1) - LOCATE('-',trim(cgi),second_occurrence -1))-1),10,16),CONV(SUBSTR(trim(cgi),LOCATE('-',trim(cgi),last_occurrence -1)+1),10,16)))) as CGI_HEX,
trim(site_name),
trim(original_i_s_date) as ORIGINAL_I,
trim(antenna_type) as ANTENNA_TY,
trim(azimuth),
trim(beamwidth),
trim(x),
trim(y),
trim(longitude),
trim(latitude),
trim(address),
trim(city),
trim(province),
NULL, --ARFCNDL
trim(bcchno),
trim(locationcode) as LOCATIONCO,
trim(bsic),
NULL, --PRIMARYSCR
'GSM', --FILE_TYPE
from_unixtime(unix_timestamp()), --INSERT_TIMESTAMP
file_date
FROM ext_cdrdm.dim_cell_site_info_gsm_temp1;

DROP TABLE IF EXISTS ext_cdrdm.dim_cell_site_info_lte_temp1;
DROP TABLE IF EXISTS ext_cdrdm.dim_cell_site_info_umts_temp1;
DROP TABLE IF EXISTS ext_cdrdm.dim_cell_site_info_gsm_temp1;