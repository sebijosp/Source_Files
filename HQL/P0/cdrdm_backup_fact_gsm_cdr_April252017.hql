SET hive.execution.engine=mr;

use cdrdm;

CREATE TABLE `ext_cdrdm.les_main_April262017_BKP`(
  `call_type` varchar(50),
  `calling_number` varchar(30),
  `called_number` varchar(30),
  `original_called_number` bigint,
  `redirecting_number` bigint,
  `chargeable_duration` int,
  `first_cell_complete` varchar(25),
  `last_cell_complete` varchar(25),
  `record_seq_number` bigint,
  `switch_name` varchar(50),
  `first_cell_id_extension` varchar(4),
  `subscriptiontype` varchar(4),
  `srvccindicator` varchar(2),
  `istbaytel` char(1),
  `call_date` string,
  `insert_timestamp` string,
  `called_serial_number` bigint,
  `calling_serial_number` bigint,
  `imsi` bigint,
  `subject_number` bigint,
  `source_table` string)
PARTITIONED BY (
  `call_timestamp_date` string)
CLUSTERED BY (
  subject_number)
SORTED BY (
  subject_number ASC)
INTO 900 BUCKETS
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');

insert into table EXT_CDRDM.les_main_April262017_BKP partition (call_timestamp_date)
select * from cdrdm.les_main;

CREATE	TABLE EXT_CDRDM.FACT_GSM_CDR_April262017_BKP(
subscriber_no           bigint,
record_sequence_number  bigint,
call_type               smallint,
imsi                    bigint,
imei                    bigint,
exchange_identity       char(15),
switch_identity         char(4),
call_timestamp          string,
chargeable_duration     int,
calling_party_number    varchar(30),
cleansed_calling_number bigint,
called_party_number     varchar(30),
cleansed_called_number  bigint,
original_called_number  varchar(30),
cleansed_original_number        bigint,
reg_seizure_charging_start      int,
mobile_station_roaming_number   bigint,
redirecting_number      varchar(30),
cleansed_redirecting_number     bigint,
fault_code              char(4),
eos_info                bigint,
incoming_route_id       varchar(7),
outgoing_route_id       varchar(7),
first_cell_id           char(14),
last_cell_id            char(14),
charged_party           smallint,
charged_party_number    varchar(30),
cleansed_charged_number bigint,
internal_cause_and_loc  char(5),
traffic_activity_code   varchar(8),
disconnecting_party     int,
partial_output_num      smallint,
rco                     char(2),
ocn                     char(4),
multimediacall          smallint,
teleservicecode         smallint,
tariffclass             smallint,
first_cell_id_extension varchar(4),
subscriptiontype        char(4),
srvccindicator          char(2),
file_name               string,
record_start            bigint,
record_end              bigint,
record_type             string,
family_name             string,
version_id              int,
file_time               string,
file_id                 string,
switch_name             string,
num_records             string,
insert_timestamp        string,
PD_Subscriber_NO	varchar(20),
TR_BAN	bigint,
TR_ACCOUNT_SUB_TYPE	char(1),
TR_COMPANY_CODE	Varchar(15),
TR_FRANCHISE_CD	Varchar(5),
TR_PRODUCT_TYPE	Varchar(1),
TR_BA_ACCOUNT_TYPE	char(1),
Sub_OCN	Varchar(4) ,
Clng_Pty_OCN	Varchar(4),
Cld_Pty_OCN	Varchar(4) ,
Sub_SPID	varchar(4),
Clng_Pty_LRN_SPID	varchar(4),
Cld_Pty_LRN_SPID	Varchar(4),
Dial_Code_Clng_Cld	Varchar(30),
Cnt_name_Clng_Cld	Varchar(150)
)	
PARTITIONED	BY(	
call_timestamp_date	string)
stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY');	


insert into table EXT_CDRDM.FACT_GSM_CDR_April262017_BKP Partition (call_timestamp_date)
select * from cdrdm.fact_gsm_cdr;


