CREATE TABLE iptv.hist_ent_viewership_metadata_ref(
FEED_TYPE string COMMENT 'from deserializer',
METADATA_TYPE string COMMENT 'from deserializer',
METADATA_VALUE string COMMENT 'from deserializer',
BUSINESS_CONTEXT string COMMENT 'from deserializer',
CONTEXT_ATTRIBUTE string COMMENT 'from deserializer'
)PARTITIONED BY (sqoop_ext_date DATE)
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY')
