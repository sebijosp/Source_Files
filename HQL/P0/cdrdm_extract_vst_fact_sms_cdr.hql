SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=mr;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;
set hive.exec.compress.output=false;


SET date_from;
SET date_to;
SET file_directory;

insert overwrite local directory '${hiveconf:file_directory}' row format delimited fields terminated by '|'
SELECT
date_yyyymm,
spid,
other_msisdn,
charged_party,
other_msisdn_sc,
count(other_msisdn) AS trafic
FROM 
(
select distinct 
subscriber_no	,
event_record_id	,
COALESCE(spid,0) as spid	,
orig_msc	,
other_msisdn	,
orig_msc_zone_indicator	,
rate	,
charged_party	,
action	,
rule	,
transaction_id	,
charge_result	,
balance	,
rate_plan	,
on_net	,
originating_service_grade	,
terminating_service_grade	,
sequence_number	,
local_subscriber_timestamp	,
imsi	,
ban	,
language	,
bill_cycle_date	,
originating_subscriber_loc	,
terminating_subscriber_loc	,
rating_rule_description_1	,
rating_rule_description_2	,
origination_eq_type	,
destination_eq_type	,
FROM_UNIXTIME(UNIX_TIMESTAMP(date_1,'yyyy-MM-dd'),'yyyyMM') as date_yyyymm	,
ocn	,
COALESCE(other_msisdn_sc,'') as other_msisdn_sc	,
tr_type_omsc	,
tr_omsc_plmn	,
tr_lac_tac	,
tr_ecid_clid	,
tr_msc_type	,
tr_dcode_country	,
tr_account_sub_type	,
tr_company_code	,
tr_franchise_cd	,
tr_ba_account_type	,
mtype	,
file_name	,
record_type	,
family_name	,
version_id	,
file_time	,
file_id	,
switch_name	,
num_records	,
local_subscriber_date
from 
CDRDM.FACT_SMS_CDR
where FACT_SMS_CDR.other_msisdn BETWEEN  0 and 999999999
AND  FACT_SMS_CDR.ACTION = 1
AND FACT_SMS_CDR.charged_party IN ('T', 'O')
AND substr(FACT_SMS_CDR.date_1,0,10) BETWEEN
'${hiveconf:date_from}'
and
 '${hiveconf:date_to}'
AND local_subscriber_date BETWEEN 
date_sub('${hiveconf:date_from}',15)
and 
date_add('${hiveconf:date_to}',15)
) sms
group BY
date_yyyymm,
spid,
other_msisdn,
charged_party,
other_msisdn_sc;

