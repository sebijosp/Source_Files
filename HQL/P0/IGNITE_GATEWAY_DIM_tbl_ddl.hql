create table iptv.Ignite_Gateway_Dim(
EQUIPMENT_STATUS        string,
EQUIPMENT_TYPE  string,
SERIAL_NUMBER   string, 
CURRENT_DATA_MAC_ID     string,
CUSTOMER_ID     string, 
EC_ID   string,
ACCOUNT_ID      string,
CITY    string,
PHUB    string, 
SHUB    string,
ZIPCODE string, 
PROVINCE        string,
SUBSCRIBER_SEQ_NBR      string, 
INSTALL_DT      timestamp, 
L9_SUBCRIBER_SUB_TYPE   string, 
L9_SAM_KEY      string
) stored as ORC;

