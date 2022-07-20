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
SET hive.tez.log.level=DEBUG;

USE cdrdm;

DROP VIEW IF EXISTS vLesSGWInfo;

CREATE VIEW IF NOT EXISTS vLesSGWInfo AS
SELECT
a.recordType, 
a.servedIMSI, 
a.sGWAddressAddress, 
a.chargingID, 
a.servingNodeAddress1,
a.servingNodeAddress2, 
a.servingNodeAddress3, 
a.servingNodeAddress4,
a.servingNodeAddress5, 
a.pdpPDNType, 
a.servedPDPPDNAddress, 
a.dataVolumeGPRSUplink1,
a.dataVolumeGPRSDownlink1, 
a.changeCondition1, 
a.changeTime1, 
a.userLocationInformation1,
a.qCI1, 
a.maxRequestedBandwithUL1, 
a.maxRequestedBandwithDL1, 
a.guaranteeBitrateUL1,
a.guaranteeBitrateDL1, 
a.aRP1, 
a.dataVolumeGPRSUplink2, 
a.dataVolumeGPRSDownlink2,
a.changeCondition2, 
a.changeTime2, 
a.userLocationInformation2, 
a.qCI2,
a.maxRequestedBandwithUL2, 
a.maxRequestedBandwithDL2, 
a.guaranteeBitrateUL2,
a.guaranteeBitrateDL2, 
a.aRP2, 
a.dataVolumeGPRSUplink3, 
a.dataVolumeGPRSDownlink3,
a.changeCondition3, 
a.changeTime3, 
a.userLocationInformation3, 
a.qCI3,
a.maxRequestedBandwithUL3, 
a.maxRequestedBandwithDL3, 
a.guaranteeBitrateUL3,
a.guaranteeBitrateDL3, 
a.aRP3, 
a.dataVolumeGPRSUplink4, 
a.dataVolumeGPRSDownlink4,
a.changeCondition4, 
a.changeTime4, 
a.userLocationInformation4, 
a.qCI4,
a.maxRequestedBandwithUL4, 
a.maxRequestedBandwithDL4, 
a.guaranteeBitrateUL4,
a.guaranteeBitrateDL4, 
a.aRP4, 
a.dataVolumeGPRSUplink5, 
a.dataVolumeGPRSDownlink5,
a.changeCondition5, 
a.changeTime5, 
a.userLocationInformation5, 
a.qCI5,
a.maxRequestedBandwithUL5, 
a.maxRequestedBandwithDL5, 
a.guaranteeBitrateUL5,
a.guaranteeBitrateDL5, 
a.aRP5, 
a.recordOpeningTime, 
a.duration, 
a.causeForRecClosing,
a.recordSequenceNumber, 
a.localSequenceNumber, 
a.servedMSISDN, 
a.chargingCHARistics,
a.servingNodePLMNIdentifier, 
a.rATType, 
a.mSTimeZone, 
a.sGWChange, 
a.servingNodeType1,
a.servingNodeType2, 
a.servingNodeType3, 
a.servingNodeType4, 
a.servingNodeType5,
a.pGWAddressUsed, 
a.pGWPLMNIdentifier, 
a.pDNConnectionID, 
a.APN, 
a.Served_IMEI,
--a.Audit_key, 
--a.Insrt_Tmstmp,
a.eCID,
--CSite.file_name,
CSite.Site,
CSite.Cell,
CSite.ANTENNA_TY,
CSite.AZIMUTH,
CSite.BEAMWIDTH,
CSite.Site_name AS First_cell_site_name, 
CSite.City AS First_cell_site_City_name,
CSite.Province AS First_cell_site_Prov_Name,
CSite.Address AS First_Cell_site_Address,
CSite.X AS First_cell_site_X,
CSite.Y AS First_cell_site_Y,
CSite.Latitude AS First_Cell_site_Latitude,
CSite.Longitude AS First_Cell_site_Longitude,
CSite.CGI AS First_Cell,
CSite.File_Date AS File_Date,
a.recordOpeningDate
FROM 
(
SELECT  
b.recordType, 
b.servedIMSI, 
b.sGWAddressAddress, 
b.chargingID, 
b.servingNodeAddress1,
b.servingNodeAddress2, 
b.servingNodeAddress3, 
b.servingNodeAddress4,
b.servingNodeAddress5, 
b.pdpPDNType, 
b.servedPDPPDNAddress, 
b.dataVolumeGPRSUplink1,
b.dataVolumeGPRSDownlink1, 
b.changeCondition1, 
b.changeTime1, 
b.userLocationInformation1,
b.qCI1, 
b.maxRequestedBandwithUL1, 
b.maxRequestedBandwithDL1, 
b.guaranteeBitrateUL1,
b.guaranteeBitrateDL1, 
b.aRP1, 
b.dataVolumeGPRSUplink2, 
b.dataVolumeGPRSDownlink2,
b.changeCondition2, 
b.changeTime2, 
b.userLocationInformation2, 
b.qCI2,
b.maxRequestedBandwithUL2, 
b.maxRequestedBandwithDL2, 
b.guaranteeBitrateUL2,
b.guaranteeBitrateDL2, 
b.aRP2, 
b.dataVolumeGPRSUplink3, 
b.dataVolumeGPRSDownlink3,
b.changeCondition3, 
b.changeTime3, 
b.userLocationInformation3, 
b.qCI3,
b.maxRequestedBandwithUL3, 
b.maxRequestedBandwithDL3, 
b.guaranteeBitrateUL3,
b.guaranteeBitrateDL3, 
b.aRP3, 
b.dataVolumeGPRSUplink4, 
b.dataVolumeGPRSDownlink4,
b.changeCondition4, 
b.changeTime4, 
b.userLocationInformation4, 
b.qCI4,
b.maxRequestedBandwithUL4, 
b.maxRequestedBandwithDL4, 
b.guaranteeBitrateUL4,
b.guaranteeBitrateDL4, 
b.aRP4, 
b.dataVolumeGPRSUplink5, 
b.dataVolumeGPRSDownlink5,
b.changeCondition5, 
b.changeTime5, 
b.userLocationInformation5, 
b.qCI5,
b.maxRequestedBandwithUL5, 
b.maxRequestedBandwithDL5, 
b.guaranteeBitrateUL5,
b.guaranteeBitrateDL5, 
b.aRP5, 
b.recordOpeningTime, 
b.duration, 
b.causeForRecClosing,
b.recordSequenceNumber, 
b.localSequenceNumber, 
b.servedMSISDN, 
b.chargingCHARistics,
b.servingNodePLMNIdentifier, 
b.rATType, 
b.mSTimeZone, 
b.sGWChange, 
b.servingNodeType1,
b.servingNodeType2, 
b.servingNodeType3, 
b.servingNodeType4, 
b.servingNodeType5,
b.pGWAddressUsed, 
b.pGWPLMNIdentifier, 
b.pDNConnectionID, 
b.APN, 
b.Served_IMEI,
b.recordOpeningDate,
--b.Audit_key, 
--b.Insrt_Tmstmp,
CONCAT(TRIM(SUBSTR(userlocationinformation1,1,3)),'-',TRIM(SUBSTR(userlocationinformation1,4,3)),'-',TRIM(SUBSTR(userlocationinformation1,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(userlocationinformation1,12,7)),10,16),'0',TRIM(SUBSTR(userlocationinformation1,19,5))),16,10)) AS eCID
FROM cdrdm.FACT_GPRS_SGW_CDR b) a LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) CGI_CELL FROM cdrdm.DIM_CELL_SITE_INFO) CSite ON a.ECID= CSite.cgi
WHERE (CGI_CELL = 1 OR CGI_CELL IS NULL);