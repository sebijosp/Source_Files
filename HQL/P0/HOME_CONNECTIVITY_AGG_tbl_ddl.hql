CREATE TABLE IF NOT EXISTS IPTV.HOME_CONNECTIVITY_AGG(
DeviceID                String          Comment 'Device id (Mac address)',
POD_Availability        Int             Comment 'Flag if Pods are active in Home on given day',
POD_Crash               Int             Comment 'Flag if Any Pod crashes in home on given day',
WIFI_QTN_Errors         Int             Comment 'Flag if Gateway have any QTN Errors',
Wifi_Error_Reboot       Int             Comment 'Flaf if Wifi Router force reboot due to Erros',
DOCSIS_Connectivity     Int             Comment 'DOCSIS Connectivity',
IP_Connectivity         Int             Comment 'IP Connectivity',
Mesh_Connectivity       Int             Comment 'Mesh Connectivity',
MoCA_Connectivity       Int             Comment 'MoCA Connectivity',
Network_Connectivity    Int             Comment 'Network Connectivity',
Process_Self_Heal       Int             Comment 'Process Self-Heal',
Process_System          Int             Comment 'Process System',
Unscheduled_Reboot_Self_Heal    Int     Comment 'Unscheduled Reboot Self-Heal',
Unscheduled_Reboot_System       Int     Comment 'Unscheduled Reboot System',
WiFi_Connectivity       Int             Comment 'WiFi Connectivity',
hdp_create_ts           Timestamp       Comment 'audit timestamp',
hdp_update_ts           Timestamp       Comment 'audit timestamp'
)PARTITIONED BY (Event_Date  Date)
STORED AS ORC;
