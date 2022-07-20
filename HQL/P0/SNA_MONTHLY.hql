SET hive.auto.convert.join=true;
SET hive.execution.engine=tez;
SET hive.default.fileformat=ORC;

drop table sna.temp_snap;
drop table sna.sna_cur;
drop table sna.sna_nw_1;
drop table sna.ctn_all;
drop table sna.all_ctn_1;

SET FIRST_DAY_OF_CURRENT_MONTH;
---------------------------

CREATE TABLE if not Exists sna.sna_current_nw(
  `ctn_orig` bigint,
  `ctn_des` bigint,
  `normalizedweight` double)
partitioned by (`sna_run_month_date` Varchar(28));

CREATE TABLE if not EXISTS sna.sna_new_weight(
  `ctn_orig` bigint,
  `ctn_des` bigint,
  `normalizedweight` double)
 partitioned by (`sna_run_month_date` varchar(28))
 STORED AS ORC;

CREATE TABLE if not EXISTS sna.sna_current(
  `ctn_orig` bigint,
  `ctn_des` bigint,
  `secs_out` bigint,
  `calls_out` bigint,
  `sms_all` bigint)
partitioned by (`sna_run_month_date` varchar(28))
 STORED AS ORC;
 
 CREATE TABLE if not EXISTS sna.sna_reference_tbl(
  `ranking` bigint,
  `hash_ed1` bigint,
  `hash_ed2` bigint,
  `hash_ed3` bigint,
  `ctn_orig` bigint)
  partitioned by (`sna_run_month_date` varchar(28))
 STORED AS ORC;
--------------------------- 

create table sna.temp_snap as
select ctn_orig, ctn_des
from sna.sna_voice;

insert into table sna.temp_snap
select ctn_orig, ctn_des
from sna.sna_sms
;


----------------HU
create table sna.sna_cur as
select a.ctn_orig, a.ctn_des, b.secs_out, b.calls_out
from (
select ctn_orig, ctn_des, count(*) as whats
from sna.temp_snap
group by ctn_orig, ctn_des) a left outer join sna.sna_voice b
   on a.ctn_orig=b.ctn_orig and a.ctn_des=b.ctn_des
;	
-----------------

----------------HU
insert overwrite table sna.sna_current partition (sna_run_month_date)
select a.*, sms_all, CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string) as sna_run_month_date
from sna.sna_cur a left outer join sna.sna_sms b
   on a.ctn_orig=b.ctn_orig and a.ctn_des=b.ctn_des	
;
-----------------

insert overwrite table sna.sna_current_nw partition (sna_run_month_date)

select ctn_orig
	, ctn_des
	,NormalizedWeight
	, CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string) as sna_run_month_date
from
(
	 SELECT CTN_ORIG
       ,CTN_DES
       ,Calls_Out
       ,Calls_In
       ,SMS_Out
       ,SMS_In
       ,Secs_out
       ,Secs_In
       ,W1
       ,W2
       ,W3
       ,W4
       ,W5
       ,W1 + W2 + W3 + W4 + W5 AS Weight

       ,CASE WHEN W1 + W2 + W3 + W4 + W5 > 1000 THEN 1000 + LOG10(W1 + W2 + W3 + W4 + W5 - 1000) ELSE W1 + W2 + W3 + W4 + W5 END AS Weight_Ceiling
       ,W6
       ,W7
       ,W8
       ,W1*COALESCE(W6, 0) AS W_OnNetCalling 
       ,W2*COALESCE(W7, 0) AS OnNetSMS 
       ,W3*COALESCE(W8, 0) AS W_OnNetSeconds 
       ,W4*COALESCE(W8, 0) AS W_CallSeconds
       ,W5*COALESCE(W7, 0) AS W_SMSNormalized
       ,W1*COALESCE(W6, 0) + W2*COALESCE(W7, 0) + W3*COALESCE(W8, 0) + W4*COALESCE(W8, 0) + W5*COALESCE(W7, 0) AS NormalizedWeight     
    
FROM (SELECT
       A.ctn_orig
       ,A.ctn_des AS ctn_des
       
       
       ,SUM(COALESCE(A.Calls_Out,0)) AS Calls_Out
       ,SUM(COALESCE(B.Calls_Out,0)) AS Calls_In
       ,SUM(COALESCE(A.sms_all,0)) AS SMS_Out
       ,SUM(COALESCE(B.sms_all,0)) AS SMS_In
       
       ,SUM(COALESCE(A.Secs_out,0)) AS Secs_out
       ,SUM(COALESCE(B.Secs_out,0)) AS Secs_In

       ,SUM(COALESCE(A.Calls_Out,0)) + SUM(COALESCE(B.Calls_Out,0)) - ABS(SUM(COALESCE(A.Calls_Out,0)) - SUM(COALESCE(B.Calls_Out,0))) AS W1
       
              --Text Message symmetry factor, suppress by factor of 10
       ,(SUM(COALESCE(A.sms_all,0)) + SUM(COALESCE(B.sms_all,0)) - ABS(SUM(COALESCE(A.sms_all,0)) - SUM(COALESCE(B.sms_all,0))))/(20.0) AS W2 
       

       ,(SUM(COALESCE(A.Secs_out,0)) + SUM(COALESCE(B.Secs_out,0)) - ABS(SUM(COALESCE(A.Secs_out,0)) - SUM(COALESCE(B.Secs_out,0))))/60.0 AS W3 
       
       ,SUM(CASE WHEN COALESCE(A.Calls_Out, 0) >= 2 THEN ((COALESCE(A.Secs_out, 0)/60.0) / COALESCE(A.Calls_Out, 0))
              WHEN COALESCE(A.Secs_out, 0)/60.0 > 5.0 THEN (COALESCE(A.Secs_out, 0)/60.0) / COALESCE(A.Calls_Out, 0)
              ELSE 0 END) AS W4
       
       ,SUM(CASE WHEN COALESCE(A.sms_all, 0) >= 5 THEN COALESCE(A.sms_all, 0)/20.0 ELSE 0 END) AS W5
       
       ,SUM(CAST(A.Calls_Out AS FLOAT)) / MAX(case when C.Total_Calls_Out=0 then NULL when C.Total_Calls_Out <>0 then C.Total_Calls_Out END) AS W6
       ,SUM(CAST(A.SMS_all AS FLOAT)) / MAX(case when C.Total_SMS_out=0 then NULL when C.Total_SMS_out <>0 then C.Total_SMS_out END) AS W7
       ,SUM(CAST(A.Secs_Out AS FLOAT)) / MAX(case when C.Total_Secs_Out=0 then NULL when C.Total_Secs_Out <>0 then C.Total_Secs_Out END) AS W8
       

FROM sna.sna_current AS A
left outer join 
(
  select ctn_orig, ctn_des, secs_out, calls_out, sms_all, sna_run_month_date
  from sna.sna_current
  where sna_run_month_date = CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string)
) AS B
ON A.ctn_orig = B.Ctn_Des
AND A.ctn_des = B.ctn_orig
left outer join (SELECT CTN_ORIG
              ,SUM(COALESCE(Calls_Out, 0)) AS Total_Calls_Out
              ,SUM(COALESCE(SMS_all, 0)) AS Total_SMS_Out
              ,SUM(COALESCE(Secs_Out, 0)) AS  Total_Secs_out
       FROM sna.sna_current
       where sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string)
       GROUP BY CTN_ORIG) AS C
ON C.CTN_ORIG = A.CTN_ORIG
where A.sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string)
GROUP BY A.ctn_orig,A.ctn_des) AS SUBQ
   ) aa 
   where ctn_orig>1000000000 and ctn_des>1000000000;


   
   
create table sna.ctn_all as
select ctn_orig
from sna.sna_current_nw where sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string);

insert into table sna.ctn_all 
select ctn_des as ctn_orig
from sna.sna_current where sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string);


create table sna.all_ctn_1 as

select cast(1 + (2000000000-1)*rand() as int) as hash_ed1
, cast(1 + (1000000000-1)*rand() as int) as hash_ed2, cast(1 + (1000000000-1)*rand() as int) as hash_ed3
, ctn_orig
from (
select ctn_orig, count(*) as de_dup
from sna.ctn_all
group by ctn_orig
) b;


insert overwrite table sna.sna_reference_tbl partition (sna_run_month_date)
select row_number() over (order by hash_ed1, hash_ed2, hash_ed3) as ranking, a.*, CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string) as sna_run_month_date from
(select * from sna.all_ctn_1 
) a;

create table sna.sna_nw_1 as
select case when a.ctn_orig=b.ctn_orig then ranking end as ctn_orig
	,ctn_des
	,NormalizedWeight

from sna.sna_current_nw a left outer join sna.sna_reference_tbl b
on a.ctn_orig=b.ctn_orig
where a.sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string) and b.sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string);



insert overwrite table sna.sna_new_weight Partition(sna_run_month_date)
select a.ctn_orig
	,case when a.ctn_des=b.ctn_orig then ranking end as ctn_des
	,NormalizedWeight, CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string) as sna_run_month_date
from sna.sna_nw_1 a left outer join sna.sna_reference_tbl b
on a.ctn_des=b.ctn_orig
where b.sna_run_month_date= CAST("${hiveconf:FIRST_DAY_OF_CURRENT_MONTH}" as string);

drop table sna.sna_sms;
drop table sna.sna_voice;
drop table sna.temp_snap;
drop table sna.sna_cur;
drop table sna.sna_nw_1;
drop table sna.ctn_all;
drop table sna.all_ctn_1;

