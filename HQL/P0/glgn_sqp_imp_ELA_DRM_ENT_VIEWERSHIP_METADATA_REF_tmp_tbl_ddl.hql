CREATE TABLE ext_iptv.ent_viewership_metadata_ref_sqp_imp (
FEED_TYPE string COMMENT 'from deserializer',
METADATA_TYPE string COMMENT 'from deserializer',
METADATA_VALUE string COMMENT 'from deserializer',
BUSINESS_CONTEXT string COMMENT 'from deserializer',
CONTEXT_ATTRIBUTE string COMMENT 'from deserializer'
)ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
