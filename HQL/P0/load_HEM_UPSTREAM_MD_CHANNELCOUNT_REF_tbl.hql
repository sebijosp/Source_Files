!echo Attempting to drop Stage table HEM.UPSTREAM_MD_CHANNELCOUNT_STG;

DROP TABLE IF EXISTS HEM.UPSTREAM_MD_CHANNELCOUNT_STG;

!echo creating HEM.UPSTREAM_MD_CHANNELCOUNT_STG;

CREATE TABLE HEM.UPSTREAM_MD_CHANNELCOUNT_STG AS
SELECT DISTINCT
   CMTS_NAME,
   MAC_DOMAIN_NAME,
   PORT_GROUP,
   CHANNEL_NAME,
   split(CHANNEL_NAME,"-")[2] as ChannelType,
   CASE WHEN MAC_DOMAIN_TYPE LIKE 'RFOG%' THEN True ELSE False END as RFOG_FLAG,
   hdp_file_name
from HEM.UPSTREAM_CHANNEL_DIM
where RECEIVED_DATE=current_date();

!echo Hive : Loading data into table HEM.UPSTREAM_MD_CHANNELCOUNT_REF;

INSERT OVERWRITE TABLE HEM.UPSTREAM_MD_CHANNELCOUNT_REF
SELECT
   CMTS_NAME,
   MAC_DOMAIN_NAME,
   PORT_GROUP,
   sum(CASE WHEN (!RFOG_FLAG and ChannelType = '6.4') THEN 1 ELSE 0 END) US_CH_COUNT_WIDE,
   sum(CASE WHEN (!RFOG_FLAG and ChannelType = '3.2') THEN 1 ELSE 0 END) US_CH_COUNT_NARROW,
   sum(CASE WHEN RFOG_FLAG THEN 1 ELSE 0 END) US_CH_COUNT_RFOG,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp()),
   HDP_FILE_NAME
FROM HEM.UPSTREAM_MD_CHANNELCOUNT_STG
GROUP BY
   CMTS_NAME,
   MAC_DOMAIN_NAME,
   PORT_GROUP,
   HDP_FILE_NAME;

!echo ;
!echo Hive : Data loaded into HEM.UPSTREAM_MD_CHANNELCOUNT_REF;
