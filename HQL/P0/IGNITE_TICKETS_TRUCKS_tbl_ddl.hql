CREATE TABLE IPTV.IGNITE_TICKETS_TRUCKS
(
X_CREATE_DATE               TIMESTAMP     COMMENT       'Work order create date'
,WORK_ORDER_ID            VARCHAR(30)     COMMENT       'Maestro work order id'
,TECH_COMPLETE_TS         TIMESTAMP       COMMENT       'Work order completion time'
,CUSTOMER_ID              VARCHAR(765)    COMMENT       'Customer id'
,SCHEDULING_SYSTEM_ID     VARCHAR(39)     COMMENT       'Scheduling system id'
,RESCHEDULE_COUNT         DECIMAL(38,18)  COMMENT       'Reschedule count'
,SCHEDULING_TEXT          VARCHAR(4000)   COMMENT       'Scheduling text & comments'
,SPECIAL_INSTRUCTIONS     VARCHAR(4000)   COMMENT       'Scheduling text & comments'
,TECH_EN_ROUTE            TIMESTAMP       COMMENT       'Technician On route timestamp'
,TECH_ON_SITE             TIMESTAMP       COMMENT       'Technician on site timestamp'
,TECH_COMPLETE            TIMESTAMP       COMMENT       'Technician work completion timestamp'
,TECH_COMMENTS            VARCHAR(4000)   COMMENT       'Technician comments'
,WOM_STATUS_TITLE         VARCHAR(80)     COMMENT       'Work order status'
,WO_STATUS                VARCHAR(255)    COMMENT       'Work order status'
,PRODUCT_LOB              VARCHAR(50)     COMMENT       'Product line of business'
,MAP_AREA_CODE            VARCHAR(5)      COMMENT       'Geography details'
,MANAGEMENT_AREA_CODE     CHAR(5)         COMMENT       'Geography details'
,FRANCHISE_AREA_CD        VARCHAR(5)      COMMENT       'Geography details'
,CBU_CODE                 CHAR(5)         COMMENT       'Geography details'
,DWELLING_TYPE_DESC       VARCHAR(50)     COMMENT       'Geography details'
,SMT                      VARCHAR(50)     COMMENT       'Geography details'
,PHUB                     VARCHAR(50)     COMMENT       'Geography details'
,NODE                     VARCHAR(50)     COMMENT       'Geography details'
,ACCOUNT_ID               VARCHAR(30)     COMMENT       'Customer Account number'
,TICKET_ID                VARCHAR(15)     COMMENT       'Ticket number'
,SUBSCRIPTION_ID          VARCHAR(25)     COMMENT       'HHID of the device'
,SERVICE_TYPE             VARCHAR(15)     COMMENT       'Service type'
,CREATE_DATE              DATE            COMMENT       'Ticket creation date'
,TICKET_CLOSE_DATETIME    DATE            COMMENT       'Ticket closing date'
,MODIFIED_DATE            DATE            COMMENT       'Ticket update date'
,CASE_LEVEL_1             VARCHAR(50)     COMMENT       'Issue categorisation hierarchy details'
,CASE_LEVEL_2             VARCHAR(50)     COMMENT       'Issue categorisation hierarchy details'
,CASE_LEVEL_3             VARCHAR(50)     COMMENT       'Issue categorisation hierarchy details'
,CASE_LEVEL_4             VARCHAR(50)     COMMENT       'Issue categorisation hierarchy details'
,CASE_LEVEL_5             VARCHAR(50)     COMMENT       'Issue categorisation hierarchy details'
,TICKET_STATUS            VARCHAR(30)     COMMENT       'Ticket status'
,HDP_INSERT_TS            TIMESTAMP       COMMENT       'hadoop insert timestamp'
,HDP_UPDATE_TS            TIMESTAMP       COMMENT       'hadoop update timestamp'

) PARTITIONED BY (TECH_COMPLETE_MONTH STRING)
STORED AS ORC;
