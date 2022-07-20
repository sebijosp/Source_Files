create table IPTV.IGNITE_GATEWAY_FIRST_ACTIVITY(
mac_id  string,
RDKB_First_Dt   Date,
HEM_First_Dt    Date,
Reconnect_First_Dt Date,
Reconnect_Last_Dt  Date,
hdp_insert_ts  timestamp,
hdp_update_ts  timestamp
)stored as ORC;
