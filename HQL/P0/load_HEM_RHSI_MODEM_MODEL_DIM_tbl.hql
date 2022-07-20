INSERT OVERWRITE TABLE HEM.RHSI_MODEM_MODEL_DIM
SELECT
   MODEM_MAKE,
   node_name as MODEM_MODEL,
   TECHNOLOGY,
   CAST(DS_TUNERS_COUNT as INT),
   CAST(US_TUNERS_COUNT as INT),
   from_unixtime(unix_timestamp()),
   from_unixtime(unix_timestamp())
FROM ela_drm.RHSI_MODEM_Metadata_ref;

