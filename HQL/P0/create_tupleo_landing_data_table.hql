create table if not exists IPTV.CMDM_RECONNECT_FCT
  (
    ACCOUNT_KEY             decimal(12,0) ,
    ACCOUNT_ID              varchar(32)   ,
    SUBSCRIBER_KEY          decimal(12,0) ,
    HHID                    varchar(50)   ,
    PRODUCT_ID              varchar(10)   ,
    CONFIRMATION_DATE       date          ,
    STB_EARLIEST_ACTIVITY   date          ,
    STB_LATEST_ACTIVITY     date
  )
 STORED AS ORC
  TBLPROPERTIES ( 
  'orc.compress'='SNAPPY',
  'orc.row.index.stride'='50000',
  'orc.stripe.size'='536870912');

create table if not exists ext_IPTV.RECONNECT_TUPLEO_CUST_TEMP
  ( 
    CONTACT_KEY         decimal(15,0) , 
    CUSTOMER_KEY        decimal(12,0) , 
    CRNT_F              char(1)       , 
    SUBSCRIBER_KEY      decimal(12,0) , 
    SUB_HHID            varchar(50)   , 
    PROD_REF_ID         varchar(10)   , 
    CONFIRMATION_DATE   date          , 
    account_key         decimal(12,0) , 
    account_id          decimal(12,0)
  )
 STORED AS ORC
  TBLPROPERTIES ( 
  'orc.compress'='SNAPPY',
  'orc.row.index.stride'='50000',
  'orc.stripe.size'='536870912');

create table if not exists IPTV.CMDM_APP_EVENTS_FCT
  (
    ACCOUNT_KEY         decimal(12,0) , 
    ACCOUNT_ID          varchar(32)   ,
    SUBSCRIBER_KEY      decimal(12,0) , 
    HHID                varchar(50)   , 
    PRODUCT_ID          varchar(10)   ,
    CONFIRMATION_DATE   Date          ,
    APP_LAUNCH_EARLIEST Date          ,
    APP_LAUNCH_COUNT    int 
  )
  STORED AS ORC
  TBLPROPERTIES ( 
  'orc.compress'='SNAPPY',
  'orc.row.index.stride'='50000',
  'orc.stripe.size'='536870912');
  
 create table if not exists ext_IPTV.APP_EVENTS_TUPLEO_CUST_TEMP
  ( 
    CONTACT_KEY         decimal(15,0) , 
    CUSTOMER_KEY        decimal(12,0) , 
    CRNT_F              char(1)       , 
    SUBSCRIBER_KEY      decimal(12,0) , 
    SUB_HHID            varchar(50)   , 
    PROD_REF_ID         varchar(10)   , 
    CONFIRMATION_DATE   date          , 
    account_key         decimal(12,0) , 
    account_id          decimal(12,0)
  )
 STORED AS ORC
  TBLPROPERTIES ( 
  'orc.compress'='SNAPPY',
  'orc.row.index.stride'='50000',
  'orc.stripe.size'='536870912'); 
 
 create table if not exists ext_IPTV.APP_EVENTS_TUPLEO_EVENT_DT_TEMP
  ( 
    HHID           varchar(50)   , 
    APP_NAME       STRING        ,
    APP_ACTION     STRING        ,     
    EVENT_DATE     date          
  )
 STORED AS ORC
  TBLPROPERTIES ( 
  'orc.compress'='SNAPPY',
  'orc.row.index.stride'='50000',
  'orc.stripe.size'='536870912');  
