CREATE TABLE iptv.ent_viewership_metadata_ref (
FEED_TYPE string COMMENT 'from deserializer',
METADATA_TYPE string COMMENT 'from deserializer',
METADATA_VALUE string COMMENT 'from deserializer',
BUSINESS_CONTEXT string COMMENT 'from deserializer',
CONTEXT_ATTRIBUTE string COMMENT 'from deserializer'
)STORED AS ORC TBLPROPERTIES ('orc.row.index.stride'='50000','orc.stripe.size'='536870912')
