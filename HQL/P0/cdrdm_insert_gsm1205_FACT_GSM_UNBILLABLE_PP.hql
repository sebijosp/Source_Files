SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
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

SET WHERE_clause;

DROP TABLE IF EXISTS ext_cdrdm.gsm1205_unbillable_temp1;
DROP TABLE IF EXISTS ext_cdrdm.gsm1205_unbillable_temp2;

CREATE TABLE IF NOT EXISTS ext_cdrdm.gsm1205_unbillable_temp1 AS
SELECT
CASE 
  WHEN calledpartynumber IS NULL THEN calledpartynumber
  ELSE
    CASE
      WHEN SUBSTR(calledpartynumber,1,5) IN ('12198','12199') THEN callednumber6_t
      ELSE
        CASE
          WHEN SUBSTR(callednumber3_t,1,16) = 0 OR SUBSTR(callednumber3_t,1,16) = '' THEN SUBSTR(callednumber3,1,16)
          ELSE SUBSTR(callednumber3_t,1,16)
        END
      END
END AS cleansedCalled,
CASE 
 WHEN RECORD_TYPE IN ('mSTerminatingSMSinMSC') THEN TRIM(serviceCentreAddress) --serviceCentreAddress instead of mscaddress
 ELSE callingpartynumber
END AS CallingNumber,
CASE 
 WHEN RECORD_TYPE IN ('mSOriginatingSMSinMSC') THEN TRIM(serviceCentreAddress) --serviceCentreAddress instead of mscaddress
 ELSE calledpartynumber
END AS CalledNumber,
originalcallednumber,
redirectingnumber,
mobilestationroamingnumber,
CASE 
 WHEN (LENGTH( TRIM (SUBSTR(IMSI,1,15))) = 0 OR SUBSTR(IMSI,1,15) IS NULL) THEN -1 
 ELSE CAST(SUBSTR(IMSI,1,15) AS BIGINT)
END IMSI,
CASE 
 WHEN (LENGTH( TRIM(SUBSTR(IMEI,1,15))) = 0 OR SUBSTR(IMEI,1,15) IS NULL)
 THEN -1 ELSE CAST(SUBSTR(IMEI,1,15) AS BIGINT)
END AS IMEI,
CASE 
 --WHEN dateforstartofcharge = '' THEN TO_DATE(CONCAT('9',SUBSTR(LPAD(CAST(SUM(1) OVER(ROWS UNBOUNDED PRECEDING) AS BIGINT),20,0),18,3),'-',FROM_UNIXTIME(UNIX_TIMESTAMP(),'MM-dd')))
 WHEN dateforstartofcharge = '' THEN file_date
 ELSE TO_DATE(CONCAT('20',SUBSTR(dateforstartofcharge,1,2),'-',SUBSTR(dateforstartofcharge,3,2),'-',SUBSTR(dateforstartofcharge,5,2)))
END AS DATE_FOR_START_OF_CHARGE,
TRIM(timeforstartofcharge) AS Time_for_Start_of_Charge,
CASE WHEN RECORD_TYPE IN ('mSOriginatingSMSinMSC', 'mSTerminatingSMSinMSC') THEN '00:01:00' 
 WHEN TRIM(RECORD_TYPE) = '' OR RECORD_TYPE IS NULL THEN '00:00:00'
ELSE chargeableduration END AS Duration,
IF(timefromregisterseizuretostartofcharging <> '',CONV(timefromregisterseizuretostartofcharging,16,10),'') AS timefromregisterseizuretostartofcharging,
CASE WHEN RECORD_TYPE IN ('mSOriginatingSMSinMSC') THEN CONV(substr(cellidforfirstcell,11,4),16,10) --TRIM(SmsMoMtRoute) 
ELSE incomingroute END AS incomingroute,
CASE WHEN RECORD_TYPE IN ('mSTerminatingSMSinMSC') THEN CONV(substr(cellidforfirstcell,11,4),16,10) --TRIM(SmsMoMtRoute) 
ELSE outgoingroute END AS outgoingroute,
cellidforfirstcell,
exchangeidentity,
switchIdentity,
Dropcode,
COALESCE (messagetypeindicator,0) AS messageTypeIndicator,
firstlocationinformationextension,
subscriptionType,
CASE WHEN TRIM(SRVCCIndicator) = 'e-UTRAN' THEN 0
WHEN TRIM(SRVCCIndicator) = 'uTRAN-HSPA' THEN 1
WHEN TRIM(SRVCCIndicator) = 'gERAN' THEN 2
WHEN TRIM(SRVCCIndicator) = 'uTRAN' THEN 3 ELSE NULL END AS SRVCCIndicator,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records
FROM 
(
 SELECT  
    COALESCE(
     (CONCAT(
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,3,1))) = HEX(LOWER(SUBSTR(calledpartynumber,3,1))) THEN SUBSTR(calledpartynumber,3,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,4,1))) = HEX(LOWER(SUBSTR(calledpartynumber,4,1))) THEN SUBSTR(calledpartynumber,4,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,5,1))) = HEX(LOWER(SUBSTR(calledpartynumber,5,1))) THEN SUBSTR(calledpartynumber,5,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
      CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END )
     ) ,'')AS callednumber3,

    REGEXP_REPLACE(COALESCE((
    CONCAT(  
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,3,1))) = HEX(LOWER(SUBSTR(calledpartynumber,3,1))) THEN SUBSTR(calledpartynumber,3,1) ELSE ''   END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,4,1))) = HEX(LOWER(SUBSTR(calledpartynumber,4,1))) THEN SUBSTR(calledpartynumber,4,1) ELSE ''   END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,5,1))) = HEX(LOWER(SUBSTR(calledpartynumber,5,1))) THEN SUBSTR(calledpartynumber,5,1) ELSE ''   END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''   END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END )),' '),'^[01]*','') AS callednumber3_t,
    
    REGEXP_REPLACE(COALESCE((
    CONCAT(
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''   END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
    CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END )),' '),'^[01]*','') AS callednumber6_t,
--RECORD_TYPE,
callingpartynumber,
calledpartynumber,
originalcallednumber,
redirectingnumber,
mobilestationroamingnumber,
IMSI,
IMEI,
dateforstartofcharge,
timeforstartofcharge,
chargeableduration,
timefromregisterseizuretostartofcharging,
incomingroute,
outgoingroute,
cellidforfirstcell,
exchangeidentity,
switchIdentity,
CASE WHEN record_type = 'transit' THEN 5008
WHEN record_type = 'mSOriginating' OR record_type = 'mSTerminating' OR record_type = 'mSOriginatingSMSinMSC' OR record_type = 'mSOriginatingSMSinSMS-IWMSC' OR record_type = 'mSTerminatingSMSinMSC' OR record_type = 'mSTerminatingSMSinSMS-GMSC' THEN 820
WHEN record_type = 'roamingCallForwarding' THEN 816
WHEN (record_type = 'iNIncomingCall' OR record_type = 'iNOutgoingCall') AND iNServiceTrigger <> '0005' THEN 800 
WHEN (record_type = 'iNIncomingCall' OR record_type = 'iNOutgoingCall') AND iNServiceTrigger = '0005' THEN 845 END AS Dropcode,
messagetypeindicator,
firstlocationinformationextension,
subscriptionType,
SRVCCIndicator,
mscaddress,
serviceCentreAddress,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
file_date
FROM cdrdm.gsm1205) a ${hiveconf:WHERE_clause} AND substr(reverse(file_name),5,2) = 'bn';

CREATE TABLE IF NOT EXISTS ext_cdrdm.gsm1205_unbillable_temp2 AS
SELECT 
CASE
 WHEN TRIM(cleansedCalled)= '' THEN NULL
 ELSE CAST(cleansedCalled AS BIGINT)
END AS SUBSCRIBER_NO,
DATE_FOR_START_OF_CHARGE AS CHANNEL_SEIZURE_DATE,
CASE
 WHEN time_for_start_of_charge = '' THEN '00:00:00'
 ELSE CAST (time_for_start_of_charge AS STRING)    
END AS CHANNEL_SEIZURE_TIME,                                                           
messagetypeindicator,
IMSI AS IMSI,
IMEI AS IMEI,
exchangeidentity AS EXCHANGE_IDENTITY,
switchIdentity AS SWITCH_IDENTITY,
DATE_FOR_START_OF_CHARGE,
CASE
 WHEN time_for_start_of_charge = '' THEN '00:00:00'
 ELSE CAST ( time_for_start_of_charge AS STRING )        
END AS TIME_FOR_START_OF_CHARGE,     
(CAST(SUBSTR(Duration,1,2) AS INT)*3600 + CAST(SUBSTR(Duration,4,2) AS INT)*60 + CAST(SUBSTR(Duration,7,2) AS INT)) AS CHARGEABLE_DURATION,
TRIM(incomingroute) AS Incoming_Route_Id,
TRIM(outgoingroute) AS Outgoing_Route_Id,   
LTRIM(callingnumber) AS CALLING_PARTY_NUMBER,
LTRIM(REGEXP_REPLACE(callednumber,'^F*|F$*','') ) AS CALLED_PARTY_NUMBER,
LTRIM(originalcallednumber) AS ORIGINAL_CALLED_NUMBER,
CASE
 WHEN (TRIM(timefromregisterseizuretostartofcharging) = '' OR timefromregisterseizuretostartofcharging IS NULL) THEN 0
 ELSE CAST(TRANSLATE(timefromregisterseizuretostartofcharging,':','') as INT)
END AS REG_SEIZURE_CHARGING_START,
LTRIM(MobileStationRoamingNumber) AS MOBILE_STATION_ROAMING_NUMBER,
LTRIM(RedirectingNumber) AS REDIRECTING_NUMBER,
TRIM(CellIDForFirstCell) AS First_Cell_Id,        
Dropcode,
firstlocationinformationextension,
subscriptionType,
SRVCCIndicator,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records
FROM ext_cdrdm.gsm1205_unbillable_temp1;

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT 
SUBSCRIBER_NO,
CHANNEL_SEIZURE_TIME,
CASE WHEN record_type = 'roamingCallForwarding' THEN 1
WHEN record_type = 'mSOriginatingSMSinMSC' THEN 2
WHEN record_type = 'mSTerminatingSMSinMSC' THEN 1 END AS call_action_code,
CASE WHEN record_type = 'mSOriginatingSMSinMSC' THEN 2
WHEN record_type = 'mSTerminatingSMSinMSC' THEN 2 END AS toll_type,
messagetypeindicator AS MESSAGE_TYPE,
NULL,
CASE WHEN record_type = 'transit' THEN 1
WHEN record_type = 'mSOriginating' THEN 2
WHEN record_type = 'roamingCallForwarding' THEN 3
WHEN record_type = 'callForwarding' THEN 4
WHEN record_type = 'mSTerminating' THEN 5
WHEN record_type = 'mSOriginatingSMSinMSC' THEN 6
WHEN record_type = 'mSOriginatingSMSinSMS-IWMSC' THEN 7
WHEN record_type = 'mSTerminatingSMSinMSC' THEN 8
WHEN record_type = 'mSTerminatingSMSinSMS-GMSC' THEN 9
WHEN record_type = 'sSProcedure' THEN 10
WHEN record_type = 'transitINOutgoingCall' THEN 11
WHEN record_type = 'iNIncomingCall' THEN 12
WHEN record_type = 'iNOutgoingCall' THEN 13
WHEN record_type = 'sCFChargingOutput' THEN 14
WHEN record_type = 'locationServices' THEN 15 END as CALL_DATA_MODULE_CHOICE_MASK,
IMSI,
IMEI,
EXCHANGE_IDENTITY,
SWITCH_IDENTITY,
DATE_FOR_START_OF_CHARGE,
TIME_FOR_START_OF_CHARGE,
CHARGEABLE_DURATION,
CALLING_PARTY_NUMBER,
CALLED_PARTY_NUMBER,
ORIGINAL_CALLED_NUMBER,
REG_SEIZURE_CHARGING_START,
MOBILE_STATION_ROAMING_NUMBER,
REDIRECTING_NUMBER,
Incoming_Route_Id,
Outgoing_Route_Id,
first_cell_id,
Dropcode AS unbillable_reason_code,
firstlocationinformationextension AS First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
CHANNEL_SEIZURE_DATE
FROM ext_cdrdm.gsm1205_unbillable_temp2 CDR;

DROP TABLE IF EXISTS ext_cdrdm.gsm1205_unbillable_temp1;
DROP TABLE IF EXISTS ext_cdrdm.gsm1205_unbillable_temp2;
