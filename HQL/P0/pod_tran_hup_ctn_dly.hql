drop table data_pods.pod_tran_hup_ctn_dly;
CREATE TABLE data_pods.pod_tran_hup_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     HUP_IND int,
     HUP_REQUEST_DATE timestamp,
     HUP_REQUEST_YEAR int,
     HUP_REQUEST_MONTH int,
     HUP_STATUS_DATE timestamp,
     HUP_YEAR int,
     HUP_MONTH int,
     HUP_N_PPC_IND int,
     hdp_update_ts timestamp)
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");
