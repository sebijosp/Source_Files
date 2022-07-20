drop table data_pods.pod_tran_ppc_ctn_dly;
CREATE TABLE data_pods.pod_tran_ppc_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     PPC_IND int,
     PPC_DATE_TYPE  string,
     PPC_REQUEST_DATE timestamp,
     PPC_REQUEST_YEAR int,
     PPC_REQUEST_MONTH int,
     PPC_EFF_DATE timestamp,
     PPC_YEAR int,
     PPC_MONTH int,
     HUP_N_PPC_IND int,
     hdp_update_ts timestamp)
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");

