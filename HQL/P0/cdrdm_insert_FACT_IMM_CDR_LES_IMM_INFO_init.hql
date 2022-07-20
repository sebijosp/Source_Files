use cdrdm;
set hive.enforce.bucketing=true;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
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
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.min.partition.factor=0.25;
set hive.tez.max.partition.factor=2.0;
SET mapred.max.split.size=1000000;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.dbclass=fs;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.exec.max.dynamic.partitions.pernode=15000;
set hive.exec.max.dynamic.partitions=15000;
set hive.exec.reducers.bytes.per.reducer=10432;
set tez.shuffle-vertex-manager.min-src-fraction=0.25;
set tez.shuffle-vertex-manager.max-src-fraction=0.75;
set tez.runtime.report.partition.stats=true;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.optimize.index.filter=true;
set hive.optimize.ppd.storage=true;

alter table cdrdm.les_imm_info rename to cdrdm.les_imm_info_bkp;
create table if not exists cdrdm.les_imm_info
(iswifiusage             string,
recordtype              char(3),
retransmission          varchar(5),
sipmethod               varchar(15),
roleofnode              char(1),
nodeaddress             varchar(200),
sessionid               varchar(200),
callingpartyaddress     varchar(20),
calledpartyaddress      varchar(20),
srvcrequesttimestamp    string,
srvcdeliverystarttimestamp      string,
srvcdeliveryendtimestamp        string,
recordopeningtime       string,
recordclosuretime       string,
srvcrequestdttmnrml     string,
srvcdeliverystartdttmnrml       string,
srvcdeliveryenddttmnrml string,
chargeableduration      smallint,
tmfromsiprqststartofchrgng      smallint,
interoperatoridentifiers        varchar(5),
localrecordsequencenumber       varchar(5),
partialoutputrecordnumber       char(3),
causeforrecordclosing   varchar(400),
acrstartlost            char(1),
acrinterimlost          char(1),
acrstoplost             char(1),
imsilost                char(1),
acrsccasstartlost       char(1),
acrsccasinterimlost     char(1),
acrsccasstoplost        char(1),
imschargingidentifier   varchar(150),
lstofsdpmediacomponents varchar(10),
lstofnrmlmediac1chgtime string,
lstofnrmlmediac1chgtimenor      string,
lstofnrmlmediac1desctype1       char(1),
lstofnrmlmediac1desccodec1      varchar(50),
lstofnrmlmediac1descbndw1       smallint,
lstofnrmlmediac1desctype2       char(1),
lstofnrmlmediac1desccodec2      varchar(50),
lstofnrmlmediac1descbndw2       smallint,
lstofnrmlmediac1desctype3       char(1),
lstofnrmlmediac1desccodec3      varchar(50),
lstofnrmlmediac1descbndw3       smallint,
lstofnrmlmediac1medinitflg      char(1),
lstofnrmlmediac2chgtime string,
lstofnrmlmediac2chgtimenor      string,
lstofnrmlmediac2desctype1       char(1),
lstofnrmlmediac2desccodec1      varchar(50),
lstofnrmlmediac2descbndw1       smallint,
lstofnrmlmediac2desctype2       char(1),
lstofnrmlmediac2desccodec2      varchar(50),
lstofnrmlmediac2descbndw2       smallint,
lstofnrmlmediac2desctype3       char(1),
lstofnrmlmediac2desccodec3      varchar(50),
lstofnrmlmediac2descbndw3       smallint,
lstofnrmlmediac2medinitflg      char(1),
lstofnrmlmediac3chgtime string,
lstofnrmlmediac3chgtimenor      string,
lstofnrmlmediac3desctype1       char(1),
lstofnrmlmediac3desccodec1      varchar(50),
lstofnrmlmediac3descbndw1       smallint,
lstofnrmlmediac3desctype2       char(1),
lstofnrmlmediac3desccodec2      varchar(50),
lstofnrmlmediac3descbndw2       smallint,
lstofnrmlmediac3desctype3       char(1),
lstofnrmlmediac3desccodec3      varchar(50),
lstofnrmlmediac3descbndw3       smallint,
lstofnrmlmediac3medinitflg      char(1),
lstofnrmlmediac4chgtime string,
lstofnrmlmediac4chgtimenor      string,
lstofnrmlmediac4desctype1       char(1),
lstofnrmlmediac4desccodec1      varchar(50),
lstofnrmlmediac4descbndw1       smallint,
lstofnrmlmediac4desctype2       char(1),
lstofnrmlmediac4desccodec2      varchar(50),
lstofnrmlmediac4descbndw2       smallint,
lstofnrmlmediac4desctype3       char(1),
lstofnrmlmediac4desccodec3      varchar(50),
lstofnrmlmediac4descbndw3       smallint,
lstofnrmlmediac4medinitflg      char(1),
lstofnrmlmediac5chgtime string,
lstofnrmlmediac5chgtimenor      string,
lstofnrmlmediac5desctype1       char(1),
lstofnrmlmediac5desccodec1      varchar(50),
lstofnrmlmediac5descbndw1       smallint,
lstofnrmlmediac5desctype2       char(1),
lstofnrmlmediac5desccodec2      varchar(50),
lstofnrmlmediac5descbndw2       smallint,
lstofnrmlmediac5desctype3       char(1),
lstofnrmlmediac5desccodec3      varchar(50),
lstofnrmlmediac5descbndw3       smallint,
lstofnrmlmediac5medinitflg      char(1),
lstofnrmlmediacomponents6       varchar(300),
lstofnrmlmediacomponents7       varchar(300),
lstofnrmlmediacomponents8       varchar(300),
lstofnrmlmediacomponents9       varchar(300),
lstofnrmlmediacomponents10      varchar(300),
lstofnrmlmediacomponents11      varchar(300),
lstofnrmlmediacomponents12      varchar(300),
lstofnrmlmediacomponents13      varchar(300),
lstofnrmlmediacomponents14      varchar(300),
lstofnrmlmediacomponents15      varchar(300),
lstofnrmlmediacomponents16      varchar(300),
lstofnrmlmediacomponents17      varchar(300),
lstofnrmlmediacomponents18      varchar(300),
lstofnrmlmediacomponents19      varchar(300),
lstofnrmlmediacomponents20      varchar(300),
lstofnrmlmediacomponents21      varchar(300),
lstofnrmlmediacomponents22      varchar(300),
lstofnrmlmediacomponents23      varchar(300),
lstofnrmlmediacomponents24      varchar(300),
lstofnrmlmediacomponents25      varchar(300),
lstofnrmlmediacomponents26      varchar(300),
lstofnrmlmediacomponents27      varchar(300),
lstofnrmlmediacomponents28      varchar(300),
lstofnrmlmediacomponents29      varchar(300),
lstofnrmlmediacomponents30      varchar(300),
lstofnrmlmediacomponents31      varchar(300),
lstofnrmlmediacomponents32      varchar(300),
lstofnrmlmediacomponents33      varchar(300),
lstofnrmlmediacomponents34      varchar(300),
lstofnrmlmediacomponents35      varchar(300),
lstofnrmlmediacomponents36      varchar(300),
lstofnrmlmediacomponents37      varchar(300),
lstofnrmlmediacomponents38      varchar(300),
lstofnrmlmediacomponents39      varchar(300),
lstofnrmlmediacomponents40      varchar(300),
lstofnrmlmediacomponents41      varchar(300),
lstofnrmlmediacomponents42      varchar(300),
lstofnrmlmediacomponents43      varchar(300),
lstofnrmlmediacomponents44      varchar(300),
lstofnrmlmediacomponents45      varchar(300),
lstofnrmlmediacomponents46      varchar(300),
lstofnrmlmediacomponents47      varchar(300),
lstofnrmlmediacomponents48      varchar(300),
lstofnrmlmediacomponents49      varchar(300),
lstofnrmlmediacomponents50      varchar(300),
lstofnrmlmedcompts1150  varchar(300),
ggsnaddress             varchar(20),
servicereasonreturncode varchar(10),
lstofmessagebodies      varchar(20),
recordextension         varchar(20),
expires                 varchar(20),
event                   varchar(20),
lst1accessnetworkinfo   varchar(26),
lst1accessdomain        varchar(2),
lst1accesstype          char(2),
lst1locationinfotype    char(1),
lst1changetime          string,
lst1changetimenormalized        string,
lst2accessnetworkinfo   varchar(26),
lst2accessdomain        varchar(2),
lst2accesstype          char(2),
lst2locationinfotype    char(1),
lst2changetime          string,
lst2changetimenormalized        string,
lst3accessnetworkinfo   varchar(26),
lst3accessdomain        varchar(2),
lst3accesstype          char(2),
lst3locationinfotype    char(1),
lst3changetime          string,
lst3changetimenormalized        string,
lst4accessnetworkinfo   varchar(26),
lst4accessdomain        varchar(2),
lst4accesstype          char(2),
lst4locationinfotype    char(1),
lst4changetime          string,
lst4changetimenormalized        string,
lst5accessnetworkinfo   varchar(26),
lst5accessdomain        varchar(2),
lst5accesstype          char(2),
lst5locationinfotype    char(1),
lst5changetime          string,
lst5changetimenormalized        string,
lst6accessnetworkinfo   varchar(26),
lst6accessdomain        varchar(2),
lst6accesstype          char(2),
lst6locationinfotype    char(1),
lst6changetime          string,
lst6changetimenormalized        string,
lst7accessnetworkinfo   varchar(26),
lst7accessdomain        varchar(2),
lst7accesstype          char(2),
lst7locationinfotype    char(1),
lst7changetime          string,
lst7changetimenormalized        string,
lst8accessnetworkinfo   varchar(26),
lst8accessdomain        varchar(2),
lst8accesstype          char(2),
lst8locationinfotype    char(1),
lst8changetime          string,
lst8changetimenormalized        string,
lst9accessnetworkinfo   varchar(26),
lst9accessdomain        varchar(2),
lst9accesstype          char(2),
lst9locationinfotype    char(1),
lst9changetime          string,
lst9changetimenormalized        string,
lst10accessnetworkinfo  varchar(26),
lst10accessdomain       varchar(2),
lst10accesstype         char(2),
lst10locationinfotype   char(1),
lst10changetime         string,
lst10changetimenormalized       string,
servicecontextid        varchar(20),
subscribere164          varchar(20),
subscriberno            bigint,
imsi                    varchar(20),
imei                    varchar(25),
subsipuri               varchar(20),
nai                     varchar(150),
subprivate              varchar(150),
subservedpartydeviceid  varchar(150),
lstofearlysdpmediacmpnts        varchar(10),
imscommserviceidentifier        varchar(10),
numberportabilityrouting        varchar(10),
carrierselectrouting    varchar(10),
sessionpriority         varchar(10),
requestedpartyaddress   varchar(40),
lstofcalledassertedid   varchar(10),
mmtelinfoanalyzedcalltype       varchar(10),
mtlinfocalledasrtidprsntsts     varchar(10),
mtlinfocllgprtyaddrprsntsts     char(2),
mmtelinfoconferenceid   varchar(5),
mmtelinfodialaroundindctr       varchar(5),
mmtelinforelatedicid    varchar(150),
mmtelsplmtrysrvcinfo1id char(4),
mmtelsplmtrysrvcinfo1act        char(4),
mmtelsplmtrysrvcinfo1redir      varchar(20),
mmtelsplmtrysrvcinfo2id char(4),
mmtelsplmtrysrvcinfo2act        char(4),
mmtelsplmtrysrvcinfo2redir      varchar(20),
mmtelinfosplmtrysrvcinfo3       varchar(150),
mmtelinfosplmtrysrvcinfo4       varchar(150),
mmtelinfosplmtrysrvcinfo5       varchar(150),
mmtelinfosplmtrysrvcinfo6       varchar(150),
mmtelinfosplmtrysrvcinfo7       varchar(150),
mmtelinfosplmtrysrvcinfo8       varchar(150),
mmtelinfosplmtrysrvcinfo9       varchar(150),
mmtelinfosplmtrysrvcinfo10      varchar(150),
mobilestationroamingnumber      varchar(128),
teleservicecode         varchar(10),
tariffclass             char(2),
pvisitednetworkid       varchar(255),
first_cell              string,
last_cell               string)
partitioned by (srvcrequestdttmnrml_date string)
clustered by (subscriberno) sorted by (subscriberno) into 900 buckets
stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

drop table if exists cdrdm.LES_IMM_INFO_STEP1;
create table cdrdm.LES_IMM_INFO_STEP1 stored as orc as
SELECT IF((CASE   WHEN  fact_imm_cdr.lst10accessdomain = '20' THEN fact_imm_cdr.lst10accessdomain  WHEN  fact_imm_cdr.lst9accessdomain = '20' THEN fact_imm_cdr.lst9accessdomain  WHEN  fact_imm_cdr.lst9accessdomain = '20' THEN fact_imm_cdr.lst8accessdomain  WHEN  fact_imm_cdr.lst7accessdomain = '20' THEN fact_imm_cdr.lst7accessdomain  WHEN  fact_imm_cdr.lst6accessdomain = '20' THEN fact_imm_cdr.lst6accessdomain  WHEN  fact_imm_cdr.lst5accessdomain = '20' THEN fact_imm_cdr.lst5accessdomain  WHEN  fact_imm_cdr.lst4accessdomain = '20' THEN fact_imm_cdr.lst4accessdomain  WHEN  fact_imm_cdr.lst3accessdomain = '20' THEN fact_imm_cdr.lst3accessdomain  WHEN  fact_imm_cdr.lst2accessdomain = '20' THEN fact_imm_cdr.lst2accessdomain  WHEN  fact_imm_cdr.lst1accessdomain = '20' THEN fact_imm_cdr.lst1accessdomain ELSE NULL END) = '20','YES','NO') as IsWifiUsage, fact_imm_cdr.recordtype,fact_imm_cdr.retransmission,fact_imm_cdr.sipmethod,fact_imm_cdr.roleofnode,fact_imm_cdr.nodeaddress,fact_imm_cdr.sessionid,fact_imm_cdr.callingpartyaddress,fact_imm_cdr.calledpartyaddress,fact_imm_cdr.srvcrequesttimestamp,fact_imm_cdr.srvcdeliverystarttimestamp,fact_imm_cdr.srvcdeliveryendtimestamp,fact_imm_cdr.recordopeningtime,fact_imm_cdr.recordclosuretime,fact_imm_cdr.srvcrequestdttmnrml,fact_imm_cdr.srvcdeliverystartdttmnrml,fact_imm_cdr.srvcdeliveryenddttmnrml,fact_imm_cdr.chargeableduration,fact_imm_cdr.tmfromsiprqststartofchrgng,fact_imm_cdr.interoperatoridentifiers,fact_imm_cdr.localrecordsequencenumber,fact_imm_cdr.partialoutputrecordnumber,fact_imm_cdr.causeforrecordclosing,fact_imm_cdr.acrstartlost,fact_imm_cdr.acrinterimlost,fact_imm_cdr.acrstoplost,fact_imm_cdr.imsilost,fact_imm_cdr.acrsccasstartlost,fact_imm_cdr.acrsccasinterimlost,fact_imm_cdr.acrsccasstoplost,fact_imm_cdr.imschargingidentifier,fact_imm_cdr.lstofsdpmediacomponents,fact_imm_cdr.lstofnrmlmediac1chgtime,fact_imm_cdr.lstofnrmlmediac1chgtimenor,fact_imm_cdr.lstofnrmlmediac1desctype1,fact_imm_cdr.lstofnrmlmediac1desccodec1,fact_imm_cdr.lstofnrmlmediac1descbndw1,fact_imm_cdr.lstofnrmlmediac1desctype2,fact_imm_cdr.lstofnrmlmediac1desccodec2,fact_imm_cdr.lstofnrmlmediac1descbndw2,fact_imm_cdr.lstofnrmlmediac1desctype3,fact_imm_cdr.lstofnrmlmediac1desccodec3,fact_imm_cdr.lstofnrmlmediac1descbndw3,fact_imm_cdr.lstofnrmlmediac1medinitflg,fact_imm_cdr.lstofnrmlmediac2chgtime,fact_imm_cdr.lstofnrmlmediac2chgtimenor,fact_imm_cdr.lstofnrmlmediac2desctype1,fact_imm_cdr.lstofnrmlmediac2desccodec1,fact_imm_cdr.lstofnrmlmediac2descbndw1,fact_imm_cdr.lstofnrmlmediac2desctype2,fact_imm_cdr.lstofnrmlmediac2desccodec2,fact_imm_cdr.lstofnrmlmediac2descbndw2,fact_imm_cdr.lstofnrmlmediac2desctype3,fact_imm_cdr.lstofnrmlmediac2desccodec3,fact_imm_cdr.lstofnrmlmediac2descbndw3,fact_imm_cdr.lstofnrmlmediac2medinitflg,fact_imm_cdr.lstofnrmlmediac3chgtime,fact_imm_cdr.lstofnrmlmediac3chgtimenor,fact_imm_cdr.lstofnrmlmediac3desctype1,fact_imm_cdr.lstofnrmlmediac3desccodec1,fact_imm_cdr.lstofnrmlmediac3descbndw1,fact_imm_cdr.lstofnrmlmediac3desctype2,fact_imm_cdr.lstofnrmlmediac3desccodec2,fact_imm_cdr.lstofnrmlmediac3descbndw2,fact_imm_cdr.lstofnrmlmediac3desctype3,fact_imm_cdr.lstofnrmlmediac3desccodec3,fact_imm_cdr.lstofnrmlmediac3descbndw3,fact_imm_cdr.lstofnrmlmediac3medinitflg,fact_imm_cdr.lstofnrmlmediac4chgtime,fact_imm_cdr.lstofnrmlmediac4chgtimenor,fact_imm_cdr.lstofnrmlmediac4desctype1,fact_imm_cdr.lstofnrmlmediac4desccodec1,fact_imm_cdr.lstofnrmlmediac4descbndw1,fact_imm_cdr.lstofnrmlmediac4desctype2,fact_imm_cdr.lstofnrmlmediac4desccodec2,fact_imm_cdr.lstofnrmlmediac4descbndw2,fact_imm_cdr.lstofnrmlmediac4desctype3,fact_imm_cdr.lstofnrmlmediac4desccodec3,fact_imm_cdr.lstofnrmlmediac4descbndw3,fact_imm_cdr.lstofnrmlmediac4medinitflg,fact_imm_cdr.lstofnrmlmediac5chgtime,fact_imm_cdr.lstofnrmlmediac5chgtimenor,fact_imm_cdr.lstofnrmlmediac5desctype1,fact_imm_cdr.lstofnrmlmediac5desccodec1,fact_imm_cdr.lstofnrmlmediac5descbndw1,fact_imm_cdr.lstofnrmlmediac5desctype2,fact_imm_cdr.lstofnrmlmediac5desccodec2,fact_imm_cdr.lstofnrmlmediac5descbndw2,fact_imm_cdr.lstofnrmlmediac5desctype3,fact_imm_cdr.lstofnrmlmediac5desccodec3,fact_imm_cdr.lstofnrmlmediac5descbndw3,fact_imm_cdr.lstofnrmlmediac5medinitflg,fact_imm_cdr.lstofnrmlmediacomponents6,fact_imm_cdr.lstofnrmlmediacomponents7,fact_imm_cdr.lstofnrmlmediacomponents8,fact_imm_cdr.lstofnrmlmediacomponents9,fact_imm_cdr.lstofnrmlmediacomponents10,fact_imm_cdr.lstofnrmlmediacomponents11,fact_imm_cdr.lstofnrmlmediacomponents12,fact_imm_cdr.lstofnrmlmediacomponents13,fact_imm_cdr.lstofnrmlmediacomponents14,fact_imm_cdr.lstofnrmlmediacomponents15,fact_imm_cdr.lstofnrmlmediacomponents16,fact_imm_cdr.lstofnrmlmediacomponents17,fact_imm_cdr.lstofnrmlmediacomponents18,fact_imm_cdr.lstofnrmlmediacomponents19,fact_imm_cdr.lstofnrmlmediacomponents20,fact_imm_cdr.lstofnrmlmediacomponents21,fact_imm_cdr.lstofnrmlmediacomponents22,fact_imm_cdr.lstofnrmlmediacomponents23,fact_imm_cdr.lstofnrmlmediacomponents24,fact_imm_cdr.lstofnrmlmediacomponents25,fact_imm_cdr.lstofnrmlmediacomponents26,fact_imm_cdr.lstofnrmlmediacomponents27,fact_imm_cdr.lstofnrmlmediacomponents28,fact_imm_cdr.lstofnrmlmediacomponents29,fact_imm_cdr.lstofnrmlmediacomponents30,fact_imm_cdr.lstofnrmlmediacomponents31,fact_imm_cdr.lstofnrmlmediacomponents32,fact_imm_cdr.lstofnrmlmediacomponents33,fact_imm_cdr.lstofnrmlmediacomponents34,fact_imm_cdr.lstofnrmlmediacomponents35,fact_imm_cdr.lstofnrmlmediacomponents36,fact_imm_cdr.lstofnrmlmediacomponents37,fact_imm_cdr.lstofnrmlmediacomponents38,fact_imm_cdr.lstofnrmlmediacomponents39,fact_imm_cdr.lstofnrmlmediacomponents40,fact_imm_cdr.lstofnrmlmediacomponents41,fact_imm_cdr.lstofnrmlmediacomponents42,fact_imm_cdr.lstofnrmlmediacomponents43,fact_imm_cdr.lstofnrmlmediacomponents44,fact_imm_cdr.lstofnrmlmediacomponents45,fact_imm_cdr.lstofnrmlmediacomponents46,fact_imm_cdr.lstofnrmlmediacomponents47,fact_imm_cdr.lstofnrmlmediacomponents48,fact_imm_cdr.lstofnrmlmediacomponents49,fact_imm_cdr.lstofnrmlmediacomponents50,fact_imm_cdr.lstofnrmlmedcompts1150,fact_imm_cdr.ggsnaddress,fact_imm_cdr.servicereasonreturncode,fact_imm_cdr.lstofmessagebodies,fact_imm_cdr.recordextension,fact_imm_cdr.expires,fact_imm_cdr.event,fact_imm_cdr.lst1accessnetworkinfo,fact_imm_cdr.lst1accessdomain,fact_imm_cdr.lst1accesstype,fact_imm_cdr.lst1locationinfotype,fact_imm_cdr.lst1changetime,fact_imm_cdr.lst1changetimenormalized,fact_imm_cdr.lst2accessnetworkinfo,fact_imm_cdr.lst2accessdomain,fact_imm_cdr.lst2accesstype,fact_imm_cdr.lst2locationinfotype,fact_imm_cdr.lst2changetime,fact_imm_cdr.lst2changetimenormalized,fact_imm_cdr.lst3accessnetworkinfo,fact_imm_cdr.lst3accessdomain,fact_imm_cdr.lst3accesstype,fact_imm_cdr.lst3locationinfotype,fact_imm_cdr.lst3changetime,fact_imm_cdr.lst3changetimenormalized,fact_imm_cdr.lst4accessnetworkinfo,fact_imm_cdr.lst4accessdomain,fact_imm_cdr.lst4accesstype,fact_imm_cdr.lst4locationinfotype,fact_imm_cdr.lst4changetime,fact_imm_cdr.lst4changetimenormalized,fact_imm_cdr.lst5accessnetworkinfo,fact_imm_cdr.lst5accessdomain,fact_imm_cdr.lst5accesstype,fact_imm_cdr.lst5locationinfotype,fact_imm_cdr.lst5changetime,fact_imm_cdr.lst5changetimenormalized,fact_imm_cdr.lst6accessnetworkinfo,fact_imm_cdr.lst6accessdomain,fact_imm_cdr.lst6accesstype,fact_imm_cdr.lst6locationinfotype,fact_imm_cdr.lst6changetime,fact_imm_cdr.lst6changetimenormalized,fact_imm_cdr.lst7accessnetworkinfo,fact_imm_cdr.lst7accessdomain,fact_imm_cdr.lst7accesstype,fact_imm_cdr.lst7locationinfotype,fact_imm_cdr.lst7changetime,fact_imm_cdr.lst7changetimenormalized,fact_imm_cdr.lst8accessnetworkinfo,fact_imm_cdr.lst8accessdomain,fact_imm_cdr.lst8accesstype,fact_imm_cdr.lst8locationinfotype,fact_imm_cdr.lst8changetime,fact_imm_cdr.lst8changetimenormalized,fact_imm_cdr.lst9accessnetworkinfo,fact_imm_cdr.lst9accessdomain,fact_imm_cdr.lst9accesstype,fact_imm_cdr.lst9locationinfotype,fact_imm_cdr.lst9changetime,fact_imm_cdr.lst9changetimenormalized,fact_imm_cdr.lst10accessnetworkinfo,fact_imm_cdr.lst10accessdomain,fact_imm_cdr.lst10accesstype,fact_imm_cdr.lst10locationinfotype,fact_imm_cdr.lst10changetime,fact_imm_cdr.lst10changetimenormalized,fact_imm_cdr.servicecontextid,fact_imm_cdr.subscribere164,fact_imm_cdr.subscriberno,fact_imm_cdr.imsi,fact_imm_cdr.imei,fact_imm_cdr.subsipuri,fact_imm_cdr.nai,fact_imm_cdr.subprivate,fact_imm_cdr.subservedpartydeviceid,fact_imm_cdr.lstofearlysdpmediacmpnts,fact_imm_cdr.imscommserviceidentifier,fact_imm_cdr.numberportabilityrouting,fact_imm_cdr.carrierselectrouting,fact_imm_cdr.sessionpriority,fact_imm_cdr.requestedpartyaddress,fact_imm_cdr.lstofcalledassertedid,fact_imm_cdr.mmtelinfoanalyzedcalltype,fact_imm_cdr.mtlinfocalledasrtidprsntsts,fact_imm_cdr.mtlinfocllgprtyaddrprsntsts,fact_imm_cdr.mmtelinfoconferenceid,fact_imm_cdr.mmtelinfodialaroundindctr,fact_imm_cdr.mmtelinforelatedicid,fact_imm_cdr.mmtelsplmtrysrvcinfo1id,fact_imm_cdr.mmtelsplmtrysrvcinfo1act,fact_imm_cdr.mmtelsplmtrysrvcinfo1redir,fact_imm_cdr.mmtelsplmtrysrvcinfo2id,fact_imm_cdr.mmtelsplmtrysrvcinfo2act,fact_imm_cdr.mmtelsplmtrysrvcinfo2redir,fact_imm_cdr.mmtelinfosplmtrysrvcinfo3,fact_imm_cdr.mmtelinfosplmtrysrvcinfo4,fact_imm_cdr.mmtelinfosplmtrysrvcinfo5,fact_imm_cdr.mmtelinfosplmtrysrvcinfo6,fact_imm_cdr.mmtelinfosplmtrysrvcinfo7,fact_imm_cdr.mmtelinfosplmtrysrvcinfo8,fact_imm_cdr.mmtelinfosplmtrysrvcinfo9,fact_imm_cdr.mmtelinfosplmtrysrvcinfo10,fact_imm_cdr.mobilestationroamingnumber,fact_imm_cdr.teleservicecode,fact_imm_cdr.tariffclass,fact_imm_cdr.pvisitednetworkid,fact_imm_cdr.srvcrequestdttmnrml_date,CASE   WHEN fact_imm_cdr.lst10accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst10accessnetworkinfo  WHEN fact_imm_cdr.lst9accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst9accessnetworkinfo WHEN fact_imm_cdr.lst8accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst8accessnetworkinfo WHEN fact_imm_cdr.lst7accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst7accessnetworkinfo WHEN fact_imm_cdr.lst6accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst6accessnetworkinfo  WHEN fact_imm_cdr.lst5accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst5accessnetworkinfo WHEN fact_imm_cdr.lst4accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst4accessnetworkinfo  WHEN fact_imm_cdr.lst3accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst3accessnetworkinfo WHEN fact_imm_cdr.lst2accessnetworkinfo IS not NULL THEN fact_imm_cdr.lst2accessnetworkinfo else null END AS LAST_CELL,
UPPER(CONCAT(SUBSTR(Lst1AccessNetworkInfo,1,3),SUBSTR(Lst1AccessNetworkInfo,5,3),SUBSTR(Lst1AccessNetworkInfo,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(Lst1AccessNetworkInfo,14)), '(0*)(.*)', 2))) AS FIRST_CELL
FROM cdrdm.FACT_IMM_CDR;

INSERT into table cdrdm.LES_IMM_INFO PARTITION (srvcrequestdttmnrml_date)
SELECT a.IsWifiUsage,a.RecordType,a.Retransmission,a.SIPMethod,a.RoleOfNode,a.NodeAddress,a.SessionId,a.CallingPartyAddress,a.CalledPartyAddress,a.SrvcRequestTimeStamp,a.SrvcDeliveryStartTimeStamp,a.SrvcDeliveryEndTimeStamp,a.RecordOpeningTime,a.RecordClosureTime,a.SrvcRequestDttmNrml,a.SrvcDeliveryStartDttmNrml,a.SrvcDeliveryEndDttmNrml,a.ChargeableDuration,a.TmFromSipRqstStartOfChrgng,a.InterOperatorIdentifiers,a.LocalRecordSequenceNumber,a.PartialOutputRecordNumber,a.CauseForRecordClosing,a.ACRStartLost,a.ACRInterimLost,a.ACRStopLost,a.IMSILost,a.ACRSCCASStartLost,a.ACRSCCASInterimLost,a.ACRSCCASStopLost,a.IMSChargingIdentifier,a.LstOfSDPMediaComponents,a.LstOfNrmlMediaC1ChgTime,a.LstOfNrmlMediaC1ChgTimeNor,a.LstOfNrmlMediaC1DescType1,a.LstOfNrmlMediaC1DescCodec1,a.LstOfNrmlMediaC1DescBndW1,a.LstOfNrmlMediaC1DescType2,a.LstOfNrmlMediaC1DescCodec2,a.LstOfNrmlMediaC1DescBndW2,a.LstOfNrmlMediaC1DescType3,a.LstOfNrmlMediaC1DescCodec3,a.LstOfNrmlMediaC1DescBndW3,a.LstOfNrmlMediaC1medInitFlg,a.LstOfNrmlMediaC2ChgTime,a.LstOfNrmlMediaC2ChgTimeNor,a.LstOfNrmlMediaC2DescType1,a.LstOfNrmlMediaC2DescCodec1,a.LstOfNrmlMediaC2DescBndW1,a.LstOfNrmlMediaC2DescType2,a.LstOfNrmlMediaC2DescCodec2,a.LstOfNrmlMediaC2DescBndW2,a.LstOfNrmlMediaC2DescType3,a.LstOfNrmlMediaC2DescCodec3,a.LstOfNrmlMediaC2DescBndW3,a.LstOfNrmlMediaC2medInitFlg,a.LstOfNrmlMediaC3ChgTime,a.LstOfNrmlMediaC3ChgTimeNor,a.LstOfNrmlMediaC3DescType1,a.LstOfNrmlMediaC3DescCodec1,a.LstOfNrmlMediaC3DescBndW1,a.LstOfNrmlMediaC3DescType2,a.LstOfNrmlMediaC3DescCodec2,a.LstOfNrmlMediaC3DescBndW2,a.LstOfNrmlMediaC3DescType3,a.LstOfNrmlMediaC3DescCodec3,a.LstOfNrmlMediaC3DescBndW3,a.LstOfNrmlMediaC3medInitFlg,a.LstOfNrmlMediaC4ChgTime,a.LstOfNrmlMediaC4ChgTimeNor,a.LstOfNrmlMediaC4DescType1,a.LstOfNrmlMediaC4DescCodec1,a.LstOfNrmlMediaC4DescBndW1,a.LstOfNrmlMediaC4DescType2,a.LstOfNrmlMediaC4DescCodec2,a.LstOfNrmlMediaC4DescBndW2,a.LstOfNrmlMediaC4DescType3,a.LstOfNrmlMediaC4DescCodec3,a.LstOfNrmlMediaC4DescBndW3,a.LstOfNrmlMediaC4medInitFlg,a.LstOfNrmlMediaC5ChgTime,a.LstOfNrmlMediaC5ChgTimeNor,a.LstOfNrmlMediaC5DescType1,a.LstOfNrmlMediaC5DescCodec1,a.LstOfNrmlMediaC5DescBndW1,a.LstOfNrmlMediaC5DescType2,a.LstOfNrmlMediaC5DescCodec2,a.LstOfNrmlMediaC5DescBndW2,a.LstOfNrmlMediaC5DescType3,a.LstOfNrmlMediaC5DescCodec3,a.LstOfNrmlMediaC5DescBndW3,a.LstOfNrmlMediaC5medInitFlg,a.LstOfNrmlMediaComponents6,a.LstOfNrmlMediaComponents7,a.LstOfNrmlMediaComponents8,a.LstOfNrmlMediaComponents9,a.LstOfNrmlMediaComponents10,a.LstOfNrmlMediaComponents11,a.LstOfNrmlMediaComponents12,a.LstOfNrmlMediaComponents13,a.LstOfNrmlMediaComponents14,a.LstOfNrmlMediaComponents15,a.LstOfNrmlMediaComponents16,a.LstOfNrmlMediaComponents17,a.LstOfNrmlMediaComponents18,a.LstOfNrmlMediaComponents19,a.LstOfNrmlMediaComponents20,a.LstOfNrmlMediaComponents21,a.LstOfNrmlMediaComponents22,a.LstOfNrmlMediaComponents23,a.LstOfNrmlMediaComponents24,a.LstOfNrmlMediaComponents25,a.LstOfNrmlMediaComponents26,a.LstOfNrmlMediaComponents27,a.LstOfNrmlMediaComponents28,a.LstOfNrmlMediaComponents29,a.LstOfNrmlMediaComponents30,a.LstOfNrmlMediaComponents31,a.LstOfNrmlMediaComponents32,a.LstOfNrmlMediaComponents33,a.LstOfNrmlMediaComponents34,a.LstOfNrmlMediaComponents35,a.LstOfNrmlMediaComponents36,a.LstOfNrmlMediaComponents37,a.LstOfNrmlMediaComponents38,a.LstOfNrmlMediaComponents39,a.LstOfNrmlMediaComponents40,a.LstOfNrmlMediaComponents41,a.LstOfNrmlMediaComponents42,a.LstOfNrmlMediaComponents43,a.LstOfNrmlMediaComponents44,a.LstOfNrmlMediaComponents45,a.LstOfNrmlMediaComponents46,a.LstOfNrmlMediaComponents47,a.LstOfNrmlMediaComponents48,a.LstOfNrmlMediaComponents49,a.LstOfNrmlMediaComponents50,a.LstOfNrmlMedCompts1150,a.GGSNAddress,a.ServiceReasonReturnCode,a.LstOfMessageBodies,a.RecordExtension,a.Expires,a.Event,a.Lst1AccessNetworkInfo,a.Lst1AccessDomain,a.Lst1AccessType,a.Lst1LocationInfoType,a.Lst1ChangeTime,a.Lst1ChangeTimeNormalized,a.Lst2AccessNetworkInfo,a.Lst2AccessDomain,a.Lst2AccessType,a.Lst2LocationInfoType,a.Lst2ChangeTime,a.Lst2ChangeTimeNormalized,a.Lst3AccessNetworkInfo,a.Lst3AccessDomain,a.Lst3AccessType,a.Lst3LocationInfoType,a.Lst3ChangeTime,a.Lst3ChangeTimeNormalized,a.Lst4AccessNetworkInfo,a.Lst4AccessDomain,a.Lst4AccessType,a.Lst4LocationInfoType,a.Lst4ChangeTime,a.Lst4ChangeTimeNormalized,a.Lst5AccessNetworkInfo,a.Lst5AccessDomain,a.Lst5AccessType,a.Lst5LocationInfoType,a.Lst5ChangeTime,a.Lst5ChangeTimeNormalized,a.Lst6AccessNetworkInfo,a.Lst6AccessDomain,a.Lst6AccessType,a.Lst6LocationInfoType,a.Lst6ChangeTime,a.Lst6ChangeTimeNormalized,a.Lst7AccessNetworkInfo,a.Lst7AccessDomain,a.Lst7AccessType,a.Lst7LocationInfoType,a.Lst7ChangeTime,a.Lst7ChangeTimeNormalized,a.Lst8AccessNetworkInfo,a.Lst8AccessDomain,a.Lst8AccessType,a.Lst8LocationInfoType,a.Lst8ChangeTime,a.Lst8ChangeTimeNormalized,a.Lst9AccessNetworkInfo,a.Lst9AccessDomain,a.Lst9AccessType,a.Lst9LocationInfoType,a.Lst9ChangeTime,a.Lst9ChangeTimeNormalized,a.Lst10AccessNetworkInfo,a.Lst10AccessDomain,a.Lst10AccessType,a.Lst10LocationInfoType,a.Lst10ChangeTime,a.Lst10ChangeTimeNormalized,a.ServiceContextID,a.SubscriberE164,a.SubscriberNo,a.IMSI,a.IMEI,a.SubSIPURI,a.NAI,a.SubPrivate,a.SubServedPartyDeviceID,a.LstOfEarlySDPMediaCmpnts,a.IMSCommServiceIdentifier,a.NumberPortabilityRouting,a.CarrierSelectRouting,a.SessionPriority,a.RequestedPartyAddress,a.LstOfCalledAssertedID,a.MMTelInfoAnalyzedCallType,a.MTlInfoCalledAsrtIDPrsntSts,a.MTlInfoCllgPrtyAddrPrsntSts,a.MMTelInfoConferenceId,a.MMTelInfoDialAroundIndctr,a.MMTelInfoRelatedICID,a.MMTelSplmtrySrvcInfo1ID,a.MMTelSplmtrySrvcInfo1Act,a.MMTelSplmtrySrvcInfo1Redir,a.MMTelSplmtrySrvcInfo2ID,a.MMTelSplmtrySrvcInfo2Act,a.MMTelSplmtrySrvcInfo2Redir,a.MMTelInfoSplmtrySrvcInfo3,a.MMTelInfoSplmtrySrvcInfo4,a.MMTelInfoSplmtrySrvcInfo5,a.MMTelInfoSplmtrySrvcInfo6,a.MMTelInfoSplmtrySrvcInfo7,a.MMTelInfoSplmtrySrvcInfo8,a.MMTelInfoSplmtrySrvcInfo9,a.MMTelInfoSplmtrySrvcInfo10,a.MobileStationRoamingNumber,a.TeleServiceCode,a.TariffClass,a.pVisitedNetworkID,a.first_cell,UPPER(CONCAT(SUBSTR(a.LAST_CELL,1,3),SUBSTR(a.LAST_CELL,5,3),SUBSTR(a.LAST_CELL,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(a.LAST_CELL,14)), '(0*)(.*)', 2))) as last_cell,A.srvcrequestdttmnrml_date
FROM cdrdm.LES_IMM_INFO_STEP1 a ;

INSERT INTO CDRDM.LES_FC_XREF PARTITION(SRC_TABLE_NAME = 'LES_IMM_INFO',CALL_TIMESTAMP_DATE)
SELECT DISTINCT A.first_Cell,SUBSTR(A.recordopeningtime,1,10), A.srvcrequestdttmnrml_date
 FROM CDRDM.LES_IMM_INFO A WHERE  A.first_Cell IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM CDRDM.LES_FC_XREF B
 WHERE B.SRC_TABLE_NAME = 'LES_IMM_INFO'
 AND B.CALL_TIMESTAMP_DATE = A.srvcrequestdttmnrml_date
 AND B.FIRST_Cell = A.FIRST_Cell)
 ;
 
 INSERT INTO CDRDM.LES_LC_XREF PARTITION(SRC_TABLE_NAME = 'LES_IMM_INFO',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT A.Last_Cell,SUBSTR(A.recordopeningtime,1,10), A.srvcrequestdttmnrml_date
 FROM CDRDM.LES_IMM_INFO A WHERE  A.Last_Cell IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM CDRDM.LES_LC_XREF B
 WHERE B.SRC_TABLE_NAME = 'LES_IMM_INFO'
 AND B.CALL_TIMESTAMP_DATE = A.srvcrequestdttmnrml_date
 AND B.Last_Cell = A.Last_Cell)
 ;

INSERT OVERWRITE table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_FC_STG  PARTITION (srvcrequestdttmnrml_date)
SELECT  A.site, A.cell, A.enodeb, A.cgi, upper(trim(A.cgi_hex)) as cgi_hex, A.site_name, A.original_i, A.antenna_ty, A.azimuth, A.beamwidth, A.x, A.y, A.longitude, A.latitude, A.address, A.city, A.province, A.arfcndl, A.bcchno, A.locationco, A.bsic, A.primaryscr, A.file_type, A.insert_timestamp, A.file_date,B.FIRST_CELL,B.CALL_TIMESTAMP_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_FC_XREF B ON B.SRC_TABLE_NAME = 'LES_IMM_INFO' AND  UPPER(TRIM(A.CGI_HEX)) = B.FIRST_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

INSERT OVERWRITE table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_lC_STG  PARTITION (srvcrequestdttmnrml_date)
SELECT  A.site, A.cell, A.enodeb, A.cgi, upper(trim(A.cgi_hex)) as cgi_hex, A.site_name, A.original_i, A.antenna_ty, A.azimuth, A.beamwidth, A.x, A.y, A.longitude, A.latitude, A.address, A.city, A.province, A.arfcndl, A.bcchno, A.locationco, A.bsic, A.primaryscr, A.file_type, A.insert_timestamp, A.file_date,B.last_CELL,B.CALL_TIMESTAMP_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_LC_XREF B ON B.SRC_TABLE_NAME = 'LES_IMM_INFO' AND UPPER(TRIM(A.CGI_HEX)) = B.last_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

drop table cdrdm.les_imm_info_bkp;
truncate table cdrdm.FACT_IMM_CDR_STG;
