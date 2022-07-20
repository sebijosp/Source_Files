--[Version History]
--0.1 - danchang - 4/06/2016 - Latest version, with a.Cdate, a.CType shown in final view on Amer's request
--0.2 - saseenthar - 6/14/2016 - Modified join predicate derivation logic (for leading zero calculation from Lst1AccessNetworkInfo) for --ext_cdrdm.v_CID_UCC_temp1 data load as part of UCC Phase 2 Requirement
--

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

USE ext_cdrdm;

DROP VIEW IF EXISTS v_CID_UCC_temp1;
DROP VIEW IF EXISTS v_CID_UCC_temp2;

CREATE VIEW IF NOT EXISTS v_CID_UCC_temp1 AS
SELECT 
SrvcRequestDttmNrml AS Cdate,
SIPMETHOD AS SIPMETHOD,
roleofnode AS roleofnode,
SrvcRequestDttmNrml AS SrvcRequestDttmNrml,
callingpartyaddress AS callingpartyaddress,
calledpartyaddress AS calledpartyaddress,
Lst1AccessNetworkInfo AS first_Cell,
MMTelSplmtrySrvcInfo1ID AS MMTelSplmtrySrvcInfo1ID,
ChargeableDuration AS ChargeableDuration,
OtherPartyAddress AS OtherPartyAddress,
ServiceProvider AS ServiceProvider,
HG_Group AS HG_Group,
SubscriberNo AS SubscriberNo,
IF(roleofnode = 0 and SIPMETHOD = 'INVITE', calledpartyaddress, IF(roleofnode = 1 and SIPMETHOD = 'INVITE', callingpartyaddress,NULL)) AS BParty,
CASE WHEN FILE_TYPE = 'GSM' THEN 'GSM_SITE'
WHEN FILE_TYPE = 'UMTS' THEN 'UMTS_SITE'
WHEN FILE_TYPE = 'LTE' THEN 'LTE_SITE' ELSE '' END AS Site_file,
CGI AS Site_CGI_DEC,
CGI_HEX AS SITE_HEX_CGI,
SITE_NAME AS SITE_NAME,
CITY AS Site_City,
CASE WHEN (roleofnode = '0' AND SIPMETHOD = 'INVITE' AND (MMTelSplmtrySrvcInfo1ID = '' OR MMTelSplmtrySrvcInfo1ID IS NULL)) THEN 'OG_CALL'
WHEN (roleofnode = '0' AND SIPMETHOD = 'INVITE' AND MMTelSplmtrySrvcInfo1ID IN ('0','1','2','3','4','5','10','11','12','13','14','15','20','30','31')) THEN 'CFW'
WHEN (roleofnode = '0' AND SIPMETHOD = 'INVITE' AND MMTelSplmtrySrvcInfo1ID IN ('901','903','903','904','905','906','907','908','909','910','911','912','913')) THEN 'Call_to_VM'
WHEN (roleofnode = '0' AND SIPMETHOD = 'MESSAGE') THEN 'OG_MSG'
WHEN (roleofnode = '1' AND SIPMETHOD = 'MESSAGE') THEN 'IC_MSG'
WHEN (roleofnode = '1' AND SIPMETHOD = 'INVITE') THEN 'IC_CALL' ELSE '' END AS CType,
CASE WHEN MMTelSplmtrySrvcInfo1ID = '9001' THEN 'Mobility'
WHEN MMTelSplmtrySrvcInfo1ID = '9002' THEN 'Shared Called Apprearence (SCA)'
WHEN MMTelSplmtrySrvcInfo1ID = '9003' THEN 'Hunt-Group'
WHEN MMTelSplmtrySrvcInfo1ID = '9004' THEN 'Deflecting (AA)'
WHEN MMTelSplmtrySrvcInfo1ID = '9005' THEN 'Sequential Ringing'
WHEN MMTelSplmtrySrvcInfo1ID = '9005' THEN 'Simultaneous Ringing' ELSE '' END AS UCC_FType,
file_date as file_date
FROM cdrdm.fact_ucc_cdr
--Phase 2 changes starts - Modified Logic
--EXTRACT AND REMOVE LEADING ZEROS IF ANY FROM Lst1AccessNetworkInfo DATA (FROM 14TH POSITION)
--LEFT JOIN cdrdm.dim_cell_site_info b ON CONCAT(SUBSTR(Lst1AccessNetworkInfo,1,3), SUBSTR(Lst1AccessNetworkInfo,5,3), SUBSTR(Lst1AccessNetworkInfo,9,4), TRIM(SUBSTR(Lst1AccessNetworkInfo,14,LENGTH(Lst1AccessNetworkInfo)))) = b.cgi_hex
LEFT JOIN cdrdm.dim_cell_site_info b ON CONCAT(SUBSTR(Lst1AccessNetworkInfo,1,3), SUBSTR(Lst1AccessNetworkInfo,5,3), SUBSTR(Lst1AccessNetworkInfo,9,4), TRIM(regexp_replace(substr(lst1accessnetworkinfo,14,length(lst1accessnetworkinfo)),'^0+',''))) = b.cgi_hex;
--Phase 2 changes ends

CREATE VIEW IF NOT EXISTS v_CID_UCC_temp2 AS 
SELECT Site_CGI_DEC, max(file_date) as max_file_date FROM ext_cdrdm.v_CID_UCC_temp1 GROUP BY Site_CGI_DEC;

USE cdrdm;

DROP VIEW IF EXISTS v_CID_UCC;

CREATE VIEW IF NOT EXISTS v_CID_UCC AS
SELECT
SUM(CAST(ROUND(a.ChargeableDuration/60) AS INT)) AS Rounded_Min,
SUM(a.ChargeableDuration) AS Total_Secs,
COUNT(DISTINCT a.SubscriberNO) AS dis_cnt_chrgPrty,
COUNT(DISTINCT a.BParty) AS dis_cnt_BParty,
COUNT(DISTINCT a.OtherPartyAddress) AS cnt_otherparty,
COUNT(a.Ctype) AS Cnt,
--date and type added to output on Amer's request
a.Cdate, a.CType
FROM ext_cdrdm.v_CID_UCC_temp1 a JOIN ext_cdrdm.v_CID_UCC_temp2 b ON a.Site_CGI_DEC = b.Site_CGI_DEC AND a.file_date = b.max_file_date 
GROUP BY a.Cdate, a.CType, a.UCC_FType, a.Site_file, a.first_Cell, a.Site_CGI_DEC, a.SITE_HEX_CGI, a.SITE_NAME, a.Site_City;