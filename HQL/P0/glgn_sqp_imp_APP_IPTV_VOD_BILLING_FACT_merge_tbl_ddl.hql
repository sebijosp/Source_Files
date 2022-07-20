create table if not exists ext_iptv.vod_billing_fact_sqp_imp_merge
(EVENT_ID_SK bigint
,EVENT_ID bigint
,VOD_TRANSACTION_ID bigint
,ASSET_SK bigint
,ASSET_ID varchar(1000)
,ASSET_ACTIVATION_DATE bigint
,ASSET_ACTIVATION_TIME_ID bigint
,ASSET_DEACTIVATION_DATE_ID bigint
,ASSET_DEACTIVATION_TIME_ID bigint
,RENTAL_DURATION varchar(255)
,RENTAL_START_DT timestamp
,RENTAL_END_DT timestamp
,PURCHASE_DATE_ID bigint
,PURCHASE_TIME_ID bigint
,CYCLE_CODE bigint
,CYCLE_INSTANCE bigint
,CYCLE_YEAR bigint
,SYS_CREATION_DATE_ID bigint
,SYS_UPDATE_DATE_ID bigint
,SYS_CREATION_TIME_ID bigint
,SYS_UPDATE_TIME_ID bigint
,EVENT_STATE varchar(1)
,DISCOUNTED_AMOUNT decimal(16,5)
,DISCOUNT_CODE varchar(15)
,DISCOUNT_NAME varchar(180)
,CANCEL_REASON_CODE varchar(5)
,CANCEL_POSTING_DATE_ID bigint
,CANCEL_POSTING_TIME_ID bigint
,PROMOTION_CODE varchar(9)
,SRC_SUBSCRIBER_ID bigint
,ACCOUNT_KEY bigint
,CUSTOMER_KEY bigint
,ADDRESS_KEY bigint
,SUBSCRIBER_KEY bigint
,SUBSCRIBER_NO varchar(30)
,SUBSCRIBER_SEQ_NBR varchar(50)
,L3_ACCOUNT int
,CUSTOMER_ID bigint
,L9_SAM_KEY varchar(13)
,L9_OFFERING_ID varchar(20)
,DEVICE_ID varchar(255)
,DEVICE_NAME varchar(20)
,CHARGED_AMOUNT decimal(16,5)
,CHARGED_TAX_AMOUNT decimal(16,5)
,TOTAL_AMOUNT_AFTER_DISCOUNT decimal(16,5)
,L3_PROVIDER_ID varchar(30)
,STATUS_CODE varchar(2)
,ETL_INSRT_RUN_ID bigint
,ETL_INSRT_DT TIMESTAMP
,ETL_UPDT_RUN_ID bigint
,ETL_UPDT_DT TIMESTAMP
,DATALAKE_EXECUTION_ID BIGINT
,DATALAKE_LOAD_TS  TIMESTAMP
) stored as orc;
