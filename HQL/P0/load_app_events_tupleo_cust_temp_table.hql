set hive.tez.container.size=8192;

with Tupleo_Cust as
(
select    DISTINCT NULL AS CONTACT_KEY,NULL AS CUSTOMER_KEY,NULL AS CRNT_F,NULL AS SUBSCRIBER_KEY,
CUSTOMER_SUBSCRIBER_NO AS SUB_HHID,PRODUCT_CODE AS PROD_REF_ID,ACTIVITY_DATE AS CONFIRMATION_DATE, CUSTOMER_ACCOUNT_KEY AS ACCOUNT_KEY
,CUSTOMER_ACCOUNT AS ACCOUNT_ID   from   APP_IBRO.IBRO_SUBSCRIBER_ACTIVITY_CLOSE where PRODUCT_CODE='PTPL' and
ACTIVITY_DATE='${hiveconf:YESTERDAY_DATE_IN}'  
)
insert OVERWRITE TABLE ext_IPTV.APP_EVENTS_TUPLEO_CUST_TEMP
SELECT  CONTACT_KEY, CUSTOMER_KEY , CRNT_F ,SUBSCRIBER_KEY , SUB_HHID ,PROD_REF_ID , CONFIRMATION_DATE , account_key , account_id FROM Tupleo_Cust;

