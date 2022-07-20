CREATE TABLE IPTV.IGNITE_CUSTOMER_CALLS
(
CONN_ID                 VARCHAR(30)  COMMENT     'Interaction id from CRM'
,ACCT_NUMBER            VARCHAR(25)  COMMENT    'Customer Account number'
,CALLTYPE1              VARCHAR(50)  COMMENT    'Customer Call Categorization type hierarchy'
,CALLTYPE2              VARCHAR(50)  COMMENT    'Customer Call Categorization type hierarchy'
,CALLTYPE3              VARCHAR(50)  COMMENT    'Customer Call Categorization type hierarchy'
,CALLTYPE4              VARCHAR(50)  COMMENT    'Customer Call Categorization type hierarchy'
,CALLTYPE5              VARCHAR(50)  COMMENT    'Customer Call Categorization type hierarchy'
,LINEOFBUSINESS         VARCHAR(40)  COMMENT    'Line of business'
,START_TIME             TIMESTAMP    COMMENT     'Start time of the call'
,END_TIME               TIMESTAMP    COMMENT    'End time of the call'
,DURATION               DECIMAL(38,18) COMMENT  'Duration of call in seconds'
,SUBSCRIPTION_ID        VARCHAR(25)  COMMENT    'HHID of the device'
,HDP_INSERT_TS          TIMESTAMP    COMMENT    'hadoop insert timestamp'
,HDP_UPDATE_TS          TIMESTAMP    COMMENT    'hadoop update timestamp'
) PARTITIONED BY (START_MONTH STRING)
STORED AS ORC;
