use cdrdm;

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
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

INSERT OVERWRITE DIRECTORY '/userapps/cdrdm/temp/fact_pgw_cdr_temp' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE 
SELECT * FROM cdrdm.pgw1215 a ${hiveconf:WHERE_clause};

DROP TABLE IF EXISTS ext_cdrdm.fact_pgw_cdr_temp1;

CREATE TABLE IF NOT EXISTS ext_cdrdm.fact_pgw_cdr_temp1(
file_name                       STRING,
record_start                    BIGINT,
record_end                      BIGINT,
record_type                     STRING,
family_name                     STRING,
version_id                      INT,
file_time                       STRING,
file_id                         STRING,
switch_name                     STRING,
num_records                     STRING,
accesspointnameni               STRING,
apnselectionmode                STRING,
causeforrecclosing              STRING,
ccrequestnumber                 STRING,
ccsrealm                        STRING,
chargingcharacteristics         STRING,
chargingid                      STRING,
chchselectionmode               STRING,
creditcontrolfailurereport      STRING,
creditcontrolsessionid          STRING,
cunt1                           STRING,
datavolumedownlink              STRING,
datavolumeuplink                STRING,
duration                        STRING,
dynamicaddressflag              STRING,
endtime                         STRING,
etsiaddress                     STRING,
externalchargingid              STRING,
ggsnaddress                     STRING,
identifier                      STRING,
imssignalingcontext             STRING,
inactivity                      STRING,
arp_1                           STRING,
arp_2                           STRING,
arp_3                           STRING,
arp_4                           STRING,
arp_5                           STRING,
datavolumegprsuplink_1          STRING,
datavolumegprsuplink_2          STRING,
datavolumegprsuplink_3          STRING,
datavolumegprsuplink_4          STRING,
datavolumegprsuplink_5          STRING,
datavolumegprsdownlink_1        STRING,
datavolumegprsdownlink_2        STRING,
datavolumegprsdownlink_3        STRING,
datavolumegprsdownlink_4        STRING,
datavolumegprsdownlink_5        STRING,
changecondition_1               STRING,
changecondition_2               STRING,
changecondition_3               STRING,
changecondition_4               STRING,
changecondition_5               STRING,
changetime_1                    STRING,
changetime_2                    STRING,
changetime_3                    STRING,
changetime_4                    STRING,
changetime_5                    STRING,
userlocationinformation_1       STRING,
userlocationinformation_2       STRING,
userlocationinformation_3       STRING,
userlocationinformation_4       STRING,
userlocationinformation_5       STRING,
epcqosinformation_1             STRING,
epcqosinformation_2             STRING,
epcqosinformation_3             STRING,
epcqosinformation_4             STRING,
epcqosinformation_5             STRING,
maxrequestedbandwithul_1        STRING,
maxrequestedbandwithul_2        STRING,
maxrequestedbandwithul_3        STRING,
maxrequestedbandwithul_4        STRING,
maxrequestedbandwithul_5        STRING,
maxrequestedbandwithdl_1        STRING,
maxrequestedbandwithdl_2        STRING,
maxrequestedbandwithdl_3        STRING,
maxrequestedbandwithdl_4        STRING,
maxrequestedbandwithdl_5        STRING,
guaranteedbitrateul_1           STRING,
guaranteedbitrateul_2           STRING,
guaranteedbitrateul_3           STRING,
guaranteedbitrateul_4           STRING,
guaranteedbitrateul_5           STRING,
guaranteedbitratedl_1           STRING,
guaranteedbitratedl_2           STRING,
guaranteedbitratedl_3           STRING,
guaranteedbitratedl_4           STRING,
guaranteedbitratedl_5           STRING,
qosnegotiated_1                 STRING,
qosnegotiated_2                 STRING,
qosnegotiated_3                 STRING,
qosnegotiated_4                 STRING,
qosnegotiated_5                 STRING,  
listofuritimestamps             STRING,
localsequencenumber             STRING,
method                          STRING,
mstimezone                      STRING,
nodeid                          STRING,
pcsrealm                        STRING,
pdptype                         STRING,
policycontrolfailurereport      STRING,
policycontrolsessionid          STRING,
ratinggroup                     STRING,
rattype                         STRING,
recordopeningtime               STRING,
recordsequencenumber            STRING,
recordtype                      STRING,
resolution                      STRING,
rulespaceid                     STRING,
servedimeisv                    STRING,
servedimsi                      STRING,
servedmsisdn                    STRING,
servedpdpaddress_iptextv6address        STRING,
servedpdpaddress_iptextv4address        STRING,
servedpdpaddress_ipbinv6address STRING,
servedpdpaddress_ipbinv4address STRING,
serviceidentifier               STRING,
servicespecificunits            STRING,
sgsnaddress                     STRING,
sgsnplmnidentifier              STRING,
significance                    STRING,
starttime                       STRING,
uri                             STRING,
uridatavolumedownlink           STRING,
uridatavolumeuplink             STRING,
uriidentifier                   STRING,
usercategory                    STRING,
userlocationinformation         STRING,
list_of_service_data            STRING,
servingnodetype                 STRING,
p_gwplmnidentifier              STRING,
stoptime                        STRING,
pdn_connection_id               STRING,
3gpp2_userlocationinformation   STRING,
served_pdp_pdn_addressext       STRING)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as textfile;

DROP TABLE IF EXISTS ext_cdrdm.fact_pgw_cdr_temp2;

CREATE TABLE IF NOT EXISTS ext_cdrdm.fact_pgw_cdr_temp2(
file_name                       STRING,
record_start                    BIGINT,
record_end                      BIGINT,
record_type                     STRING,
family_name                     STRING,
version_id                      INT,
file_time                       STRING,
file_id                         STRING,
switch_name                     STRING,
num_records                     STRING,
accesspointnameni               STRING,
apnselectionmode                STRING,
causeforrecclosing              STRING,
ccrequestnumber                 STRING,
ccsrealm                        STRING,
chargingcharacteristics         STRING,
chargingid                      STRING,
chchselectionmode               STRING,
creditcontrolfailurereport      STRING,
creditcontrolsessionid          STRING,
cunt1                           STRING,
datavolumedownlink              STRING,
datavolumeuplink                STRING,
duration                        STRING,
dynamicaddressflag              STRING,
endtime                         STRING,
etsiaddress                     STRING,
externalchargingid              STRING,
ggsnaddress                     STRING,
identifier                      STRING,
imssignalingcontext             STRING,
inactivity                      STRING,
listoftrafficvolumes            array<STRING>,
listofuritimestamps             STRING,
localsequencenumber             STRING,
method                          STRING,
mstimezone                      STRING,
nodeid                          STRING,
pcsrealm                        STRING,
pdptype                         STRING,
policycontrolfailurereport      STRING,
policycontrolsessionid          STRING,
ratinggroup                     STRING,
rattype                         STRING,
recordopeningtime               STRING,
recordsequencenumber            STRING,
recordtype                      STRING,
resolution                      STRING,
rulespaceid                     STRING,
servedimeisv                    STRING,
servedimsi                      STRING,
servedmsisdn                    STRING,
servedpdpaddress_iptextv6address        STRING,
servedpdpaddress_iptextv4address        STRING,
servedpdpaddress_ipbinv6address STRING,
servedpdpaddress_ipbinv4address STRING,
serviceidentifier               STRING,
servicespecificunits            STRING,
sgsnaddress                     STRING,
sgsnplmnidentifier              STRING,
significance                    STRING,
starttime                       STRING,
uri                             STRING,
uridatavolumedownlink           STRING,
uridatavolumeuplink             STRING,
uriidentifier                   STRING,
usercategory                    STRING,
userlocationinformation         STRING,
list_of_service_data            STRING,
servingnodetype                 STRING,
p_gwplmnidentifier              STRING,
stoptime                        STRING,
pdn_connection_id               STRING,
3gpp2_userlocationinformation   STRING,
served_pdp_pdn_addressext       STRING,
file_date                       STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ':' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

LOAD DATA INPATH '/userapps/cdrdm/temp/fact_pgw_cdr_temp/*' INTO TABLE ext_cdrdm.fact_pgw_cdr_temp2;

INSERT INTO TABLE cdrdm.fact_pgw_cdr PARTITION(recordopeningdate)
SELECT file_name,record_start,record_end,record_type,family_name,version_id,cast (file_time as TIMESTAMP) as file_time,file_id,switch_name,num_records,accesspointnameni,apnselectionmode,
causeforrecclosing,ccrequestnumber,ccsrealm,chargingcharacteristics,chargingid,chchselectionmode,creditcontrolfailurereport,creditcontrolsessionid,
cunt1,datavolumedownlink,datavolumeuplink,duration,dynamicaddressflag,endtime,etsiaddress,externalchargingid,ggsnaddress,identifier,imssignalingcontext,
inactivity,
case
when (regexp_extract(listoftrafficvolumes[0],'arp=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'arp=(\.*?)(\;)', 1) 
end as arp_1,
case
when (regexp_extract(listoftrafficvolumes[1],'arp=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'arp=(\.*?)(\;)', 1) 
end as arp_2,
case 
when (regexp_extract(listoftrafficvolumes[2],'arp=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'arp=(\.*?)(\;)', 1) 
end as arp_3,
case
when (regexp_extract(listoftrafficvolumes[3],'arp=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'arp=(\.*?)(\;)', 1) 
end as arp_4,
case 
when (regexp_extract(listoftrafficvolumes[4],'arp=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'arp=(\.*?)(\;)', 1)
end  as arp_5,
case
when (regexp_extract(listoftrafficvolumes[0],'datavolumegprsuplink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'datavolumegprsuplink=(\.*?)(\;)', 1) 
end  as datavolumegprsuplink_1,
case
when (regexp_extract(listoftrafficvolumes[1],'datavolumegprsuplink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'datavolumegprsuplink=(\.*?)(\;)', 1) 
end as datavolumegprsuplink_2,
case 
when (regexp_extract(listoftrafficvolumes[2],'datavolumegprsuplink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'datavolumegprsuplink=(\.*?)(\;)', 1) 
end as datavolumegprsuplink_3,
case 
when (regexp_extract(listoftrafficvolumes[3],'datavolumegprsuplink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'datavolumegprsuplink=(\.*?)(\;)', 1) 
end as datavolumegprsuplink_4,
case
when (regexp_extract(listoftrafficvolumes[4],'datavolumegprsuplink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'datavolumegprsuplink=(\.*?)(\;)', 1) 
end as datavolumegprsuplink_5,
case
when (regexp_extract(listoftrafficvolumes[0],'datavolumegprsdownlink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'datavolumegprsdownlink=(\.*?)(\;)', 1) 
end as datavolumegprsdownlink_1,
case 
when (regexp_extract(listoftrafficvolumes[1],'datavolumegprsdownlink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'datavolumegprsdownlink=(\.*?)(\;)', 1) 
end as datavolumegprsdownlink_2,
case
when (regexp_extract(listoftrafficvolumes[2],'datavolumegprsdownlink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'datavolumegprsdownlink=(\.*?)(\;)', 1) 
end as datavolumegprsdownlink_3,
case
when (regexp_extract(listoftrafficvolumes[3],'datavolumegprsdownlink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'datavolumegprsdownlink=(\.*?)(\;)', 1) 
end as datavolumegprsdownlink_4,
case
when (regexp_extract(listoftrafficvolumes[4],'datavolumegprsdownlink=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'datavolumegprsdownlink=(\.*?)(\;)', 1) 
end as datavolumegprsdownlink_5,
case
when (regexp_extract(listoftrafficvolumes[0],'changecondition=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'changecondition=(\.*?)(\;)', 1) 
end as changecondition_1,
case 
when (regexp_extract(listoftrafficvolumes[1],'changecondition=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'changecondition=(\.*?)(\;)', 1) 
end as changecondition_2,
case 
when (regexp_extract(listoftrafficvolumes[2],'changecondition=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'changecondition=(\.*?)(\;)', 1) 
end as changecondition_3,
case 
when (regexp_extract(listoftrafficvolumes[3],'changecondition=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'changecondition=(\.*?)(\;)', 1) 
end as changecondition_4,
case
when (regexp_extract(listoftrafficvolumes[4],'changecondition=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'changecondition=(\.*?)(\;)', 1) 
end as changecondition_5,
case 
when (regexp_extract(listoftrafficvolumes[0],'changetime=(\.*?)(\;)', 1)) = "" then NULL
else from_unixtime(unix_timestamp(substr(regexp_extract(listoftrafficvolumes[0],'changetime=(\.*?)(\;)', 1),1,12),'yyMMddHHmmss')) 
end as changetime_1,
case
when (regexp_extract(listoftrafficvolumes[1],'changetime=(\.*?)(\;)', 1)) = "" then NULL
else from_unixtime(unix_timestamp(substr(regexp_extract(listoftrafficvolumes[1],'changetime=(\.*?)(\;)', 1),1,12),'yyMMddHHmmss')) 
end as changetime_2,
case 
when (regexp_extract(listoftrafficvolumes[2],'changetime=(\.*?)(\;)', 1)) = "" then NULL
else from_unixtime(unix_timestamp(substr(regexp_extract(listoftrafficvolumes[2],'changetime=(\.*?)(\;)', 1),1,12),'yyMMddHHmmss'))  
end as changetime_3,
case
when (regexp_extract(listoftrafficvolumes[3],'changetime=(\.*?)(\;)', 1)) = "" then NULL
else from_unixtime(unix_timestamp(substr(regexp_extract(listoftrafficvolumes[3],'changetime=(\.*?)(\;)', 1),1,12),'yyMMddHHmmss'))  
end as changetime_4,
case
when (regexp_extract(listoftrafficvolumes[4],'changetime=(\.*?)(\;)', 1)) = "" then NULL
else from_unixtime(unix_timestamp(substr(regexp_extract(listoftrafficvolumes[4],'changetime=(\.*?)(\;)', 1),1,12),'yyMMddHHmmss')) 
end as changetime_5,
case
when (regexp_extract(listoftrafficvolumes[0],'userlocationinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'userlocationinformation=(\.*?)(\;)', 1)
end  as userlocationinformation_1,
case
when (regexp_extract(listoftrafficvolumes[1],'userlocationinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'userlocationinformation=(\.*?)(\;)', 1) 
end as userlocationinformation_2,
case 
when (regexp_extract(listoftrafficvolumes[2],'userlocationinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'userlocationinformation=(\.*?)(\;)', 1) 
end as userlocationinformation_3,
case
when (regexp_extract(listoftrafficvolumes[3],'userlocationinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'userlocationinformation=(\.*?)(\;)', 1) 
end as userlocationinformation_4,
case
when (regexp_extract(listoftrafficvolumes[4],'userlocationinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'userlocationinformation=(\.*?)(\;)', 1)
end  as userlocationinformation_5,
case
when (regexp_extract(listoftrafficvolumes[0],'epcqosinformation=(\.*?)(\;)', 1))  = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'epcqosinformation=(\.*?)(\;)', 1) 
end as epcqosinformation_1,
case
when (regexp_extract(listoftrafficvolumes[1],'epcqosinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'epcqosinformation=(\.*?)(\;)', 1) 
end as epcqosinformation_2,
case
when (regexp_extract(listoftrafficvolumes[2],'epcqosinformation=(\.*?)(\;)', 1))  = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'epcqosinformation=(\.*?)(\;)', 1) 
end as epcqosinformation_3,
case
when (regexp_extract(listoftrafficvolumes[3],'epcqosinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'epcqosinformation=(\.*?)(\;)', 1) 
end as epcqosinformation_4,
case
when (regexp_extract(listoftrafficvolumes[4],'epcqosinformation=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'epcqosinformation=(\.*?)(\;)', 1) 
end as epcqosinformation_5,
case
when (regexp_extract(listoftrafficvolumes[0],'maxrequestedbandwithul=(\.*?)(\;)', 1))  = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'maxrequestedbandwithul=(\.*?)(\;)', 1) 
end as maxrequestedbandwithul_1,
case
when (regexp_extract(listoftrafficvolumes[1],'maxrequestedbandwithul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'maxrequestedbandwithul=(\.*?)(\;)', 1) 
end as maxrequestedbandwithul_2,
case
when (regexp_extract(listoftrafficvolumes[2],'maxrequestedbandwithul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'maxrequestedbandwithul=(\.*?)(\;)', 1) 
end as maxrequestedbandwithul_3,
case
when (regexp_extract(listoftrafficvolumes[3],'maxrequestedbandwithul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'maxrequestedbandwithul=(\.*?)(\;)', 1) 
end as maxrequestedbandwithul_4,
case
when (regexp_extract(listoftrafficvolumes[4],'maxrequestedbandwithul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'maxrequestedbandwithul=(\.*?)(\;)', 1) 
end as maxrequestedbandwithul_5,
case
when (regexp_extract(listoftrafficvolumes[0],'maxrequestedbandwithdl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'maxrequestedbandwithdl=(\.*?)(\;)', 1) 
end as maxrequestedbandwithdl_1,
case
when (regexp_extract(listoftrafficvolumes[1],'maxrequestedbandwithdl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'maxrequestedbandwithdl=(\.*?)(\;)', 1) 
end as maxrequestedbandwithdl_2,
case
when (regexp_extract(listoftrafficvolumes[2],'maxrequestedbandwithdl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'maxrequestedbandwithdl=(\.*?)(\;)', 1) 
end as maxrequestedbandwithdl_3,
case
when (regexp_extract(listoftrafficvolumes[3],'maxrequestedbandwithdl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'maxrequestedbandwithdl=(\.*?)(\;)', 1) 
end as maxrequestedbandwithdl_4,
case
when (regexp_extract(listoftrafficvolumes[4],'maxrequestedbandwithdl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'maxrequestedbandwithdl=(\.*?)(\;)', 1) 
end as maxrequestedbandwithdl_5,
case
when (regexp_extract(listoftrafficvolumes[0],'guaranteedbitrateul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'guaranteedbitrateul=(\.*?)(\;)', 1) 
end as guaranteedbitrateul_1,
case
when (regexp_extract(listoftrafficvolumes[1],'guaranteedbitrateul=(\.*?)(\;)', 1)) = "" then NULL 
else regexp_extract(listoftrafficvolumes[1],'guaranteedbitrateul=(\.*?)(\;)', 1) 
end as guaranteedbitrateul_2,
case
when (regexp_extract(listoftrafficvolumes[2],'guaranteedbitrateul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'guaranteedbitrateul=(\.*?)(\;)', 1) 
end as guaranteedbitrateul_3,
case
when (regexp_extract(listoftrafficvolumes[3],'guaranteedbitrateul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'guaranteedbitrateul=(\.*?)(\;)', 1) 
end as guaranteedbitrateul_4,
case
when (regexp_extract(listoftrafficvolumes[4],'guaranteedbitrateul=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'guaranteedbitrateul=(\.*?)(\;)', 1) 
end as guaranteedbitrateul_5,
case
when (regexp_extract(listoftrafficvolumes[0],'guaranteedbitratedl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'guaranteedbitratedl=(\.*?)(\;)', 1) 
end as guaranteedbitratedl_1,
case
when (regexp_extract(listoftrafficvolumes[1],'guaranteedbitratedl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'guaranteedbitratedl=(\.*?)(\;)', 1) 
end as guaranteedbitratedl_2,
case
when (regexp_extract(listoftrafficvolumes[2],'guaranteedbitratedl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[2],'guaranteedbitratedl=(\.*?)(\;)', 1) 
end as guaranteedbitratedl_3,
case
when (regexp_extract(listoftrafficvolumes[3],'guaranteedbitratedl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'guaranteedbitratedl=(\.*?)(\;)', 1) 
end as guaranteedbitratedl_4,
case
when (regexp_extract(listoftrafficvolumes[4],'guaranteedbitratedl=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'guaranteedbitratedl=(\.*?)(\;)', 1) 
end as guaranteedbitratedl_5,
case
when (regexp_extract(listoftrafficvolumes[0],'qosnegotiated=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[0],'qosnegotiated=(\.*?)(\;)', 1)
end  as qosnegotiated_1,
case
when (regexp_extract(listoftrafficvolumes[1],'qosnegotiated=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[1],'qosnegotiated=(\.*?)(\;)', 1) 
end as qosnegotiated_2,
case 
when (regexp_extract(listoftrafficvolumes[2],'qosnegotiated=(\.*?)(\;)', 1)) = "" then NULL 
else regexp_extract(listoftrafficvolumes[2],'qosnegotiated=(\.*?)(\;)', 1) 
end as qosnegotiated_3,
case
when (regexp_extract(listoftrafficvolumes[3],'qosnegotiated=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[3],'qosnegotiated=(\.*?)(\;)', 1) 
end as qosnegotiated_4,
case
when (regexp_extract(listoftrafficvolumes[4],'qosnegotiated=(\.*?)(\;)', 1)) = "" then NULL
else regexp_extract(listoftrafficvolumes[4],'qosnegotiated=(\.*?)(\;)', 1) 
end as qosnegotiated_5,
listofuritimestamps,
localsequencenumber,
method,
mstimezone,
nodeid,
pcsrealm,
pdptype,
policycontrolfailurereport,
policycontrolsessionid,
ratinggroup,
rattype,
from_unixtime(unix_timestamp(substr(recordopeningtime,1,12),'yyMMddHHmmss')) as recordopeningtime,
recordsequencenumber,
recordtype,
resolution,
rulespaceid,
servedimeisv,
servedimsi,
servedmsisdn,
servedpdpaddress_iptextv6address,
servedpdpaddress_iptextv4address,
servedpdpaddress_ipbinv6address,
servedpdpaddress_ipbinv4address,
serviceidentifier,
servicespecificunits,
sgsnaddress,
sgsnplmnidentifier,
significance,
from_unixtime(unix_timestamp(substr(starttime,1,12),'yyMMddHHmmss')) as starttime,
uri,uridatavolumedownlink,
uridatavolumeuplink,
uriidentifier,
usercategory,
userlocationinformation,
list_of_service_data,
servingnodetype,
p_gwplmnidentifier,
from_unixtime(unix_timestamp(substr(stoptime,1,12),'yyMMddHHmmss')) as stoptime,
pdn_connection_id,3gpp2_userlocationinformation,served_pdp_pdn_addressext,
cast(file_date as DATE),
concat('20',substr(recordopeningtime,1,2),'-',substr(recordopeningtime,3,2),'-',substr(recordopeningtime,5,2)) as recordopeningdate
FROM ext_cdrdm.fact_pgw_cdr_temp2;

TRUNCATE TABLE ext_cdrdm.fact_pgw_cdr_temp2;
TRUNCATE TABLE ext_cdrdm.fact_pgw_cdr_temp1;
