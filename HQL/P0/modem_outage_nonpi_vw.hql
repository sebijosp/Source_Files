CREATE OR REPLACE VIEW `hem_nonpi.vw_modem_outage` AS
SELECT if (cm_mac_addr IS NOT NULL,cast (reflect ('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (cm_mac_addr AS STRING))) AS VARCHAR(64)),NULL) AS cm_mac_addr,                  
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
;
