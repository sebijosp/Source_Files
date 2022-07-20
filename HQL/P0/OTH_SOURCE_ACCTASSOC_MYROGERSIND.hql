use oth_source;


CREATE TABLE IF NOT EXISTS oth_source.ACCTASSOC_MYROGERSIND
(
cn varchar(32)
,acctnbr char(12)
,accttypenbr int
,UserId VARCHAR(320)
,assocDt TIMESTAMP
,assocpin char(4)
,assocPinExpiryDt TIMESTAMP
,assocPinValidatedDt TIMESTAMP
,printbillflag int
,sendemailflag int
,onlinebillviewflag int
,acctemail varchar(250)
,billingday int
,assocpinstrikes int
,addrline1 varchar(60)
,addrline2 varchar(60)
,addrline3 varchar(60)
,addrline4 varchar(60)
,addrline5 varchar(60)
,addrcity varchar(28)
,addrpostalcd varchar(11)
,langcd char(2)
,vestcomSentDt TIMESTAMP
,ebppflag int
,ProcessDt TIMESTAMP
,ebppEnrollDt TIMESTAMP
,ebppDeEnrollDt TIMESTAMP
,marketingflag int
,infochanged int
,operatorid varchar(32)
,epoststatus int
,ePostStatusDt TIMESTAMP
,ctn varchar(10)
,passcode varchar(4)
,assocmethod varchar(18)
,dpsflag varchar(10)
,DPSFlagDt TIMESTAMP
,rtichecked char(1)
,RTICheckedDt TIMESTAMP
,epostsupplementalflag int
,LastSyncDate TIMESTAMP
,acctEmailUpdateDt TIMESTAMP
)
stored as ORC ;



Truncate table oth_source.ACCTASSOC_MYROGERSIND;


INSERT INTO oth_source.ACCTASSOC_MYROGERSIND
SELECT
A.cn
,A.acctnbr
,A.accttypenbr
,null
,A.assocDt
,A.assocpin
,A.assocPinExpiryDt
,A.assocPinValidatedDt
,A.printbillflag
,A.sendemailflag
,A.onlinebillviewflag
,A.acctemail
,A.billingday
,A.assocpinstrikes
,A.addrline1
,A.addrline2
,A.addrline3
,A.addrline4
,A.addrline5
,A.addrcity
,A.addrpostalcd
,A.langcd
,A.vestcomSentDt
,A.ebppflag
,A.ProcessDt
,A.ebppEnrollDt
,A.ebppDeEnrollDt
,A.marketingflag
,A.infochanged
,A.operatorid
,A.epoststatus
,A.ePostStatusDt
,A.ctn
,A.passcode
,A.assocmethod
,A.dpsflag
,A.DPSFlagDt
,A.rtichecked
,A.RTICheckedDt
,A.epostsupplementalflag
,A.LastSyncDate
,A.acctEmailUpdateDt
FROM ACCTASSOC A ;
