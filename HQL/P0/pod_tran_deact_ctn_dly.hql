drop table data_pods.pod_tran_deact_ctn_dly;
CREATE TABLE data_pods.pod_tran_deact_ctn_dly(
     ECID bigint,
     BAN bigint,
     SUBSCRIBER_NO bigint,
     DEACT_IND int,
     DEACT_DATE timestamp,
     DEACT_YEAR int,
     DEACT_MONTH int,
     CHURN_CODE  string,
     CHURN_DESC  string,
     VOLUNTARY_CODE  string,
     VOLUNTARY_DESC  string,
     DEACT_REASON_CODE  string,
     DEACT_REASON_DESC  string,
     PORT_OUT  string,
     INTERBRAND_PORT_OUT  string,
     INTERBRAND_PREPAID_IND  string,
     NSP  string,
     CARRIER_CHURN_CODE  string,
     CARRIER_CHURN_DESC  string,
     CARRIER_SHORT_NAME  string,
     hdp_update_ts timestamp)
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");





