-- MODEM_ACCESSIBILITY_DAILY is partitioned hence check the bkp table manually
select count(1) from HEM.MODEM_ACCESSIBILITY_DAILY;
ALTER TABLE HEM.MODEM_ACCESSIBILITY_DAILY RENAME TO HEM.MODEM_ACCESSIBILITY_DAILY_BKP_22102019;
select count(1) from HEM.MODEM_ACCESSIBILITY_DAILY_BKP_22102019;

drop table if exists hem.s06_distinct_mac_perday;
create table if not exists hem.s06_distinct_mac_perday as select distinct cm_mac_addr, processed_date, cmts_md_if_name, cmts_md_if_index, cmts_host_name from hem.ipdr_s06;

create table if not exists HEM.MODEM_ACCESSIBILITY_DAILY_DEDUPED like HEM.MODEM_ACCESSIBILITY_DAILY_BKP_22102019;
truncate table HEM.MODEM_ACCESSIBILITY_DAILY_DEDUPED;

INSERT overwrite TABLE HEM.MODEM_ACCESSIBILITY_DAILY_DEDUPED partition ( event_date )
SELECT daily.mac,
       daily.cmts,
       daily.account,
       daily.full_account_number,
       daily.cbu_code,
       daily.system,
       daily.phub,
       daily.shub,
       daily.cmts_md_if_index,
       daily.cmts_md_if_name,
       daily.node,
       daily.rtn_seg,
       daily.clamshell,
       daily.smt,
       daily.modem_manufacturer,
       daily.model,
       daily.firmware,
       daily.bb_tier,
       daily.hh_type,
       daily.contract_type,
       daily.address,
       daily.time_zone,
       daily.is_dst,
       daily.equipment_status_code,
       daily.billing_src_mac_id,
       daily.postal_zip_code,
       daily.outage_count,
       daily.up_status_count,
       daily.down_status_count,
       daily.downtime_all_day,
       daily.total_time_all_day,
       daily.accessibility_perc_all_day,
       daily.downtime_prime,
       daily.total_time_prime,
       daily.accessibility_perc_prime,
       daily.downtime_full_day,
       daily.total_time_full_day,
       daily.accessibility_perc_full_day,
       daily.hdp_insert_ts,
       daily.hdp_update_ts,
       daily.event_date
from (select *,
row_Number() over (partition by mac, cmts, account,full_account_number,cbu_code, system,phub, shub, node, rtn_seg,clamshell, smt,modem_manufacturer, model,firmware, bb_tier,hh_type, contract_type,address,time_zone,is_dst,equipment_status_code,billing_src_mac_id,postal_zip_code, event_date ) as row_num
from hem.MODEM_ACCESSIBILITY_DAILY_BKP_22102019 ) daily
where row_num = 1 ;
CREATE TABLE HEM.MODEM_ACCESSIBILITY_DAILY  like HEM.MODEM_ACCESSIBILITY_DAILY_BKP_22102019;
INSERT overwrite TABLE HEM.MODEM_ACCESSIBILITY_DAILY partition ( event_date )
SELECT daily.mac,
       daily.cmts,
       daily.account,
       daily.full_account_number,
       daily.cbu_code,
       daily.system,
       daily.phub,
       daily.shub,
       s06.cmts_md_if_index,
       s06.cmts_md_if_name,
       daily.node,
       daily.rtn_seg,
       daily.clamshell,
       daily.smt,
       daily.modem_manufacturer,
       daily.model,
       daily.firmware,
       daily.bb_tier,
       daily.hh_type,
       daily.contract_type,
       daily.address,
       daily.time_zone,
       daily.is_dst,
       daily.equipment_status_code,
       daily.billing_src_mac_id,
       daily.postal_zip_code,
       daily.outage_count,
       daily.up_status_count,
       daily.down_status_count,
       daily.downtime_all_day,
       daily.total_time_all_day,
       daily.accessibility_perc_all_day,
       daily.downtime_prime,
       daily.total_time_prime,
       daily.accessibility_perc_prime,
       daily.downtime_full_day,
       daily.total_time_full_day,
       daily.accessibility_perc_full_day,
       daily.hdp_insert_ts,
       daily.hdp_update_ts,
       daily.event_date
FROM   HEM.MODEM_ACCESSIBILITY_DAILY_DEDUPED daily LEFT OUTER JOIN hem.s06_distinct_mac_perday s06 ON s06.cm_mac_addr = daily.mac and daily.event_date = s06.processed_date and s06.cmts_host_name = daily.cmts;
