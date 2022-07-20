create table if not exists iptv.hist_vod_credit_fact
(CREDIT_ID_SK bigint
, EVENT_ID bigint
, VOD_TRANSACTION_ID bigint
, CREDIT_ID bigint
, FK_EVENT_ID_SK bigint
, ASSET_SK bigint
, REFUND_REASON_CODE varchar(10)
, REFUND_REASON varchar(150)
, PURCHASE_DATE_ID bigint
, RENTAL_DURATION varchar(255)
, RENTAL_START_DT timestamp
, RENTAL_END_DT timestamp
, SYS_CREATION_DATE_ID bigint
, SYS_UPDATE_DATE_ID bigint
, CANCEL_POSTING_DATE_ID bigint
, PURCHASE_TIME_ID bigint
, SYS_CREATION_TIME_ID bigint
, SYS_UPDATE_TIME_ID bigint
, CANCEL_POSTING_TIME_ID bigint
, REFUND_AMOUNT decimal(18,2)
, REFUNDED_TAX_AMOUNT decimal(18,2)
, ACCOUNT_KEY bigint
, CUSTOMER_KEY bigint
, ADDRESS_KEY bigint
, SUBSCRIBER_KEY bigint
, SUBSCRIBER_NO varchar(30)
, SUBSCRIBER_SEQ_NBR varchar(50)
, ASSET_ID varchar(1000)
, ASSET_ACTIVATION_DATE bigint
, ASSET_ACTIVATION_TIME_ID bigint
, ASSET_DEACTIVATION_DATE_ID bigint
, ASSET_DEACTIVATION_TIME_ID bigint
, L3_PROVIDER_ID varchar(30)
, ETL_INSRT_RUN_ID bigint
, ETL_INSRT_DT timestamp
, ETL_UPDT_RUN_ID bigint
, ETL_UPDT_DT timestamp
,DATALAKE_EXECUTION_ID BIGINT
,DATALAKE_LOAD_TS  TIMESTAMP
) partitioned by (sqoop_ext_date date) stored as orc;
