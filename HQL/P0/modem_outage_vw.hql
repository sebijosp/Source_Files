CREATE OR REPLACE VIEW `hem.modem_outage_vw` AS
SELECT cm_mac_addr,                  
down_start_ts,
down_end_ts,
interval_start_ts,
interval_end_ts,
downtime,
hdp_insert_ts,
hdp_update_ts,
job_execution_id,
interval_date
from  hem.modem_outage
WHERE interval_date = DATE_SUB(CURRENT_DATE(), 1);
