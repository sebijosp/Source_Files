INSERT OVERWRITE TABLE HEM.PRODUCT_TIER_REFERENCE
SELECT
   node_name as PRODUCT_CODE,
   TIER,
   TIER_TYPE,
   tier_detail AS TIER_DETAILS,
   OVERAGE_THRESHOLD_GB ,
   SERVICE_CLASS,
   DOWNLOAD_SPEED_KBPS,
   UPLOAD_SPEED_KBPS,
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp())
FROM ela_drm.PRODUCT_TIER_REF;
