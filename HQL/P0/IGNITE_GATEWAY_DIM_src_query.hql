!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Dropping temp table;

DROP TABLE IF EXISTS iptv.equipment_temp;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Creating temp table;

create table iptv.equipment_temp
(Equipment_status   string, 
EQUIPMENT_TYPE   string,
SERIAL_NUMBER string, 
CURRENT_DATA_MAC_ID  string,
CUSTOMER_KEY  string,
ACCOUNT_KEY  string,
ADDRESS_KEY  string,
TRANSFER_DATE  timestamp,
TRANSFER_DATE_ROW_NUM  int
) stored as ORC;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Loading data into temp table;

insert overwrite table iptv.equipment_temp
Select Equipment_status, 
EQUIPMENT_TYPE,
SERIAL_NUMBER, 
CURRENT_DATA_MAC_ID,
CUSTOMER_KEY,
ACCOUNT_KEY,
ADDRESS_KEY,
TRANSFER_DATE,
ROW_NUMBER() OVER (PARTITION BY CURRENT_DATA_MAC_ID ORDER BY TRANSFER_DATE  DESC)
from app_maestro.eqmnttrkrfct
where current_data_mac_id is not null;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Temp table load completed !;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Loading target table !;

insert overwrite table iptv.Ignite_Gateway_Dim
Select distinct eqmnt.Equipment_status, 
eqmnt.EQUIPMENT_TYPE,
eqmnt.SERIAL_NUMBER, 
eqmnt.CURRENT_DATA_MAC_ID,
cust.CUSTOMER_ID, 
cust.EC_ID,
acct.ACCOUNT_ID,
addr.CITY, addr.PHUB, 
addr.SHUB,addr.ZIPCODE, 
addr.PROVINCE,
sbscr.SUBSCRIBER_SEQ_NBR, 
sbscr.INSTALL_DT, 
sbscr.L9_SUBCRIBER_SUB_TYPE, 
sbscr.L9_SAM_KEY
from
  iptv.equipment_temp eqmnt
  left join app_maestro.custdim cust
  on eqmnt.customer_key=cust.customer_key 
  left join app_maestro.acctdim acct
  on eqmnt.account_key=acct.account_key 
  left join app_maestro.addrdim addr
  on eqmnt.address_key=addr.address_key
  left join app_maestro.sbscrbrdim sbscr
  on eqmnt.customer_key=sbscr.customer_key
  and eqmnt.address_key=sbscr.address_key 
where addr.current_address_serviceable = 'Y'
and cust.crnt_f='Y'
and acct.crnt_f='Y'
and addr.crnt_f='Y'
and sbscr.crnt_f='Y'
and sbscr.subscriber_type = 'IP'
and eqmnt.transfer_date_row_num = 1;

!date +%Y-%m-%dT%H:%M:%S;!echo INFO Hive: Target table load completed !;
