CREATE TABLE cdrdm.fido_data_bytes
(
hashed_ban varchar(64),
hashed_ctn varchar(64),
ban varchar(20),
ctn varchar(20),
trans_dt timestamp,
interaction_start_ts timestamp,
interaction_end_ts timestamp,
duration bigint,
data_source varchar(64),
event_description string,
interaction_outcome string,
data_mb_used decimal(6,2),
product_group varchar(64),
trans_channel_level_1 varchar(32),
trans_channel_level_2 varchar(32),
account_type varchar(32),
account_subtype varchar(32),
disposition_reason_cd_1 string,
disposition_reason_cd_3 varchar(32)
)
PARTITIONED BY (
partition_dt date)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'