drop table data_pods.pod_tran_nac_ctn_dly;
CREATE TABLE data_pods.pod_tran_nac_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     NAC_IND int,
     NAC_DATE_TYPE  string,
     INIT_ACTIVATION_DATE timestamp,
     NAC_YEAR int,
     NAC_MONTH int,
     NAC_OSP  string,
     NAC_CARRIER_SHORT_NAME  string,
     INTERBRAND_PORT  string,
     hdp_update_ts timestamp)
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");

