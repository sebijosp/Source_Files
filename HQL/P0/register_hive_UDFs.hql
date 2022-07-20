
CREATE FUNCTION iptv.trickplayEventOffsetCount as 'com.rogers.dmt.bdi.iptv.hiveudf.TrickplayEventOffsetCount' USING JAR 'hdfs://edwbiprod/user/hdpiptv/UDFs/IPTVHiveUDFs.jar';

CREATE FUNCTION iptv.trickplayEventOffsetSum as 'com.rogers.dmt.bdi.iptv.hiveudf.TrickplayEventOffsetSum' USING JAR 'hdfs://edwbiprod/user/hdpiptv/UDFs/IPTVHiveUDFs.jar';

CREATE FUNCTION iptv.concatArrayToString as 'com.rogers.dmt.bdi.iptv.hiveudf.ConcatArrayToString' USING JAR 'hdfs://edwbiprod/user/hdpiptv/UDFs/IPTVHiveUDFs.jar';

