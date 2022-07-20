drop view CDRDM.V_INBOUND_GSM_CDR;
drop view CDRDM.V_OUTBOUND_GSM_CDR;
drop view CDRDM.V_INBOUND_GSM_TRANSIT_CDR;
drop view CDRDM.V_OUTBOUND_GSM_TRANSIT_CDR;


create view CDRDM.V_INBOUND_GSM_CDR as
select
subscriber_no,
record_sequence_number,
call_type,
imsi,
imei,
exchange_identity,
switch_identity,
call_timestamp,
chargeable_duration,
calling_party_number,
cleansed_calling_number as A_NUMBER,
called_party_number,
cleansed_called_number as B_NUMBER,
original_called_number,
cleansed_original_number,
reg_seizure_charging_start,
mobile_station_roaming_number,
redirecting_number,
cleansed_redirecting_number,
fault_code,
eos_info,
incoming_route_id as inbound_route_name,
outgoing_route_id as outbound_route_name,
first_cell_id,
last_cell_id,
charged_party,
charged_party_number,
cleansed_charged_number,
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
partial_output_num,
rco,
ocn,
multimediacall,
teleservicecode,
tariffclass,
first_cell_id_extension,
subscriptiontype,
srvccindicator,
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
insert_timestamp,
PD_Subscriber_NO,
TR_BAN,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_PRODUCT_TYPE,
TR_BA_ACCOUNT_TYPE,
Sub_OCN,
Clng_Pty_OCN,
Cld_Pty_OCN,
Sub_SPID,
Clng_Pty_LRN_SPID,
Cld_Pty_LRN_SPID,
Dial_Code_Clng_Cld,
Cnt_name_Clng_Cld,
call_timestamp_date,
routeID,
msc,
mgw,
route_name,
carrier,
term_switch,
dir,
type,
sub_type,
loc_prov,
location,
lir,
mar,
ds0,
route_file_name,
date_loaded,
telecom_company_of_calling_pty_ocn_desc as calling_pty_ocn_desc,
telecom_company_of_calling_pty_abbr_ocn_desc as calling_pty_abbr_ocn_desc,
telecom_company_of_calling_pty_ocn_state as calling_pty_ocn_state,
telecom_company_of_calling_pty_category as calling_pty_category,
telecom_company_of_called_pty_ocn_desc as called_pty_ocn_desc,
telecom_company_of_called_pty_abbr_ocn_desc as called_pty_abbr_ocn_desc,
telecom_company_of_called_pty_ocn_state as called_pty_ocn_state,
telecom_company_of_called_pty_category as called_pty_category,
sub_spid_ocn_match,
clng_pty_spid_ocn_match,
cld_pty_spid_ocn_match,
incoming_or_outgoing_cdr,
concatenated_outgoing_route_details,
'IN' as Direction,
if(clng_pty_ocn is NULL, clng_pty_lrn_spid, if(clng_pty_lrn_spid is NULL, clng_pty_ocn, if(clng_pty_ocn is not NULL and clng_pty_lrn_spid is not NULL and trim(clng_pty_ocn) <> trim(clng_pty_lrn_spid), clng_pty_lrn_spid,  clng_pty_lrn_spid))) as A_OCN_SPID ,
if(cld_pty_ocn is NULL, cld_pty_lrn_spid, if(cld_pty_lrn_spid is NULL, cld_pty_ocn, if(cld_pty_ocn is not NULL and cld_pty_lrn_spid is not NULL and trim(cld_pty_ocn) <> trim(cld_pty_lrn_spid), cld_pty_lrn_spid,  cld_pty_lrn_spid))) as B_OCN_SPID, 
if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 1, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 1, 3),NULL)) as A_NPA,

if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 4, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 4, 3),NULL)) as A_NXX,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 1, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 1, 3),NULL)) as B_NPA,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 4, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 4, 3),NULL)) as B_NXX

from(
select
cdr.subscriber_no,
cdr.record_sequence_number,
cdr.call_type,
cdr.imsi,
cdr.imei,
cdr.exchange_identity,
cdr.switch_identity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.reg_seizure_charging_start,
cdr.mobile_station_roaming_number,
cdr.redirecting_number,
cdr.cleansed_redirecting_number,
cdr.fault_code,
cdr.eos_info,
cdr.incoming_route_id,
cdr.outgoing_route_id,
cdr.first_cell_id,
cdr.last_cell_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internal_cause_and_loc,
cdr.traffic_activity_code,
cdr.disconnecting_party,
cdr.partial_output_num,
cdr.rco,
cdr.ocn,
cdr.multimediacall,
cdr.teleservicecode,
cdr.tariffclass,
cdr.first_cell_id_extension,
cdr.subscriptiontype,
cdr.srvccindicator,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
cdr.insert_timestamp,
cdr.PD_Subscriber_NO,
cdr.TR_BAN,
cdr.TR_ACCOUNT_SUB_TYPE,
cdr.TR_COMPANY_CODE,
cdr.TR_FRANCHISE_CD,
cdr.TR_PRODUCT_TYPE,
cdr.TR_BA_ACCOUNT_TYPE,
cdr.Sub_OCN,
cdr.Clng_Pty_OCN,
cdr.Cld_Pty_OCN,
cdr.Sub_SPID,
cdr.Clng_Pty_LRN_SPID,
cdr.Cld_Pty_LRN_SPID,
cdr.Dial_Code_Clng_Cld,
cdr.Cnt_name_Clng_Cld,
cdr.call_timestamp_date,

if(cdr.call_type in (2),nvl(out_routes.routeID,in_routes.routeID), if(cdr.call_type in (3, 4, 5), nvl(in_routes.routeID, out_routes.routeID), NULL)) as routeID,
if(cdr.call_type in (2),nvl(out_routes.MSC,in_routes.MSC),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.MSC,out_routes.MSC), NULL)) as MSC,
if(cdr.call_type in (2),nvl(out_routes.MGW,in_routes.MGW),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.MGW,out_routes.MGW), NULL)) as MGW,
if(cdr.call_type in (2),nvl(out_routes.Route_Name,in_routes.Route_Name),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Route_Name,out_routes.Route_Name), NULL)) as Route_Name,
if(cdr.call_type in (2),nvl(out_routes.Carrier,in_routes.Carrier),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Carrier,out_routes.Carrier), NULL)) as Carrier,
if(cdr.call_type in (2),nvl(out_routes.Term_Switch,in_routes.Term_Switch),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Term_Switch,out_routes.Term_Switch), NULL)) as Term_Switch,
if(cdr.call_type in (2),nvl(out_routes.DIR,in_routes.DIR),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.DIR,out_routes.DIR), NULL)) as DIR,
if(cdr.call_type in (2),nvl(out_routes.Type,in_routes.Type),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Type,out_routes.Type), NULL)) as Type,
if(cdr.call_type in (2),nvl(out_routes.Sub_Type,in_routes.Sub_Type),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Sub_Type,out_routes.Sub_Type), NULL)) as Sub_Type,
if(cdr.call_type in (2),nvl(out_routes.Loc_Prov,in_routes.Loc_Prov),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Loc_Prov,out_routes.Loc_Prov), NULL)) as Loc_Prov,
if(cdr.call_type in (2),nvl(out_routes.Location,in_routes.Location),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Location,out_routes.Location), NULL)) as Location,
if(cdr.call_type in (2),nvl(out_routes.LIR,in_routes.LIR),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.LIR,out_routes.LIR), NULL)) as LIR,
if(cdr.call_type in (2),nvl(out_routes.MAR,in_routes.MAR),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.MAR,out_routes.MAR), NULL)) as MAR,
if(cdr.call_type in (2),nvl(out_routes.DS0,in_routes.DS0),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.DS0,out_routes.DS0), NULL)) as DS0,
if(cdr.call_type in (2),nvl(out_routes.File_Name,in_routes.File_Name),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.File_Name,out_routes.File_Name), NULL)) as Route_File_Name,
if(cdr.call_type in (2),nvl(out_routes.Date_loaded,in_routes.Date_loaded), if(cdr.call_type in (3, 4, 5), nvl(in_routes.Date_loaded,out_routes.Date_loaded), NULL)) as Date_loaded,

if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ocn_desc, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ocn_desc, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ocn_desc,  lerg_calling_spid.ocn_desc))) as telecom_company_of_calling_pty_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ABBR_OCN_DESC, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ABBR_OCN_DESC, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ABBR_OCN_DESC,  lerg_calling_spid.ABBR_OCN_DESC))) as telecom_company_of_calling_pty_ABBR_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.OCN_STATE, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.OCN_STATE, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.OCN_STATE,  lerg_calling_spid.OCN_STATE))) as telecom_company_of_calling_pty_OCN_STATE,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.CATEGORY, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.CATEGORY, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.CATEGORY,  lerg_calling_spid.CATEGORY))) as telecom_company_of_calling_pty_CATEGORY,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ocn_desc, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ocn_desc, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ocn_desc,  lerg_called_spid.ocn_desc))) as telecom_company_of_called_pty_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ABBR_OCN_DESC, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ABBR_OCN_DESC, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ABBR_OCN_DESC,  lerg_called_spid.ABBR_OCN_DESC))) as telecom_company_of_called_pty_ABBR_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.OCN_STATE, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.OCN_STATE, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.OCN_STATE,  lerg_called_spid.OCN_STATE))) as telecom_company_of_called_pty_OCN_STATE,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.CATEGORY, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.CATEGORY, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.CATEGORY,  lerg_called_spid.CATEGORY))) as telecom_company_of_called_pty_CATEGORY,
if(cdr.sub_spid = cdr.sub_ocn, 'Y', 'N') as sub_spid_ocn_match,
if(cdr.clng_pty_lrn_spid = cdr.clng_pty_ocn, 'Y', 'N') as clng_pty_spid_ocn_match,
if(cdr.cld_pty_lrn_spid = cdr.cld_pty_ocn, 'Y', 'N') as cld_pty_spid_ocn_match,

CASE when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH'
when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT'
when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL
ELSE 'IN'
END as incoming_or_outgoing_cdr,

CASE when (case when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH' when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT' when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL ELSE 'IN' END) = 'BOTH' then  
concat_ws('|',out_routes.routeid, out_routes.msc, out_routes.mgw, out_routes.route_name, out_routes.carrier, out_routes.term_switch, out_routes.dir, out_routes.type, out_routes.sub_type,
out_routes.loc_prov, out_routes.location, out_routes.lir, out_routes.mar, out_routes.ds0, out_routes.file_name, out_routes.date_loaded) END as concatenated_outgoing_route_details


from cdrdm.fact_gsm_cdr cdr

left outer join cdrdm.DIM_BK_ROUTES in_routes
ON
(CASE WHEN UPPER(substr(CDR.incoming_route_id, -1)) = 'I' THEN substr(trim(CDR.incoming_route_id), 1, length(trim(CDR.incoming_route_id)) - 1 ) ELSE CDR.incoming_route_id END) = trim(in_routes.Route_Name)
left outer join cdrdm.DIM_BK_ROUTES out_routes
ON
(CASE WHEN UPPER(substr(CDR.outgoing_route_id, -1)) = 'O' THEN substr(trim(CDR.outgoing_route_id), 1, length(trim(CDR.outgoing_route_id)) - 1 ) ELSE CDR.outgoing_route_id END)= trim(out_routes.Route_Name)

Left outer Join (select tt.OCN, tt.OCN_Desc, tt.ABBR_OCN_DESC, tt.OCN_State, tt.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) t from ela_v21.lerg1_ocn) tt where tt.t=1) lerg_called_spid
ON trim(cdr.cld_pty_lrn_spid) = trim(lerg_called_spid.ocn)
Left outer Join (select ss.OCN, ss.OCN_Desc, ss.ABBR_OCN_DESC, ss.OCN_State, ss.Category  from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) s from ela_v21.lerg1_ocn) ss where ss.s=1) lerg_called_ocn
ON trim(cdr.cld_pty_ocn) = trim(lerg_called_ocn.ocn)
Left outer Join (select rr.OCN, rr.OCN_Desc, rr.ABBR_OCN_DESC, rr.OCN_State, rr.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) r from ela_v21.lerg1_ocn) rr where rr.r=1) lerg_calling_spid
ON trim(cdr.clng_pty_lrn_spid) = trim(lerg_calling_spid.ocn)
Left outer Join (select qq.OCN, qq.OCN_Desc, qq.ABBR_OCN_DESC, qq.OCN_State, qq.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) q from ela_v21.lerg1_ocn) qq where qq.q=1) lerg_calling_ocn
ON trim(cdr.clng_pty_ocn) = trim(lerg_calling_ocn.ocn)
where cdr.call_type in (2,3,4,5)
) xxx
where incoming_or_outgoing_cdr in ('IN', 'BOTH');



create view CDRDM.V_OUTBOUND_GSM_CDR as
select
subscriber_no,
record_sequence_number,
call_type,
imsi,
imei,
exchange_identity,
switch_identity,
call_timestamp,
chargeable_duration,
calling_party_number,
cleansed_calling_number as A_NUMBER,
called_party_number,
cleansed_called_number as B_NUMBER,
original_called_number,
cleansed_original_number,
reg_seizure_charging_start,
mobile_station_roaming_number,
redirecting_number,
cleansed_redirecting_number,
fault_code,
eos_info,
incoming_route_id as inbound_route_name,
outgoing_route_id as outbound_route_name,
first_cell_id,
last_cell_id,
charged_party,
charged_party_number,
cleansed_charged_number,
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
partial_output_num,
rco,
ocn,
multimediacall,
teleservicecode,
tariffclass,
first_cell_id_extension,
subscriptiontype,
srvccindicator,
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
insert_timestamp,
PD_Subscriber_NO,
TR_BAN,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_PRODUCT_TYPE,
TR_BA_ACCOUNT_TYPE,
Sub_OCN,
Clng_Pty_OCN,
Cld_Pty_OCN,
Sub_SPID,
Clng_Pty_LRN_SPID,
Cld_Pty_LRN_SPID,
Dial_Code_Clng_Cld,
Cnt_name_Clng_Cld,
call_timestamp_date,
IF(incoming_or_outgoing_cdr='OUT', routeid, split(concatenated_outgoing_route_details,'\\|')[0]) as routeid,
IF(incoming_or_outgoing_cdr='OUT', msc, split(concatenated_outgoing_route_details,'\\|')[1]) as msc,
IF(incoming_or_outgoing_cdr='OUT', mgw, split(concatenated_outgoing_route_details,'\\|')[2]) as mgw,
IF(incoming_or_outgoing_cdr='OUT', route_name, split(concatenated_outgoing_route_details,'\\|')[3]) as route_name,
IF(incoming_or_outgoing_cdr='OUT', carrier, split(concatenated_outgoing_route_details,'\\|')[4]) as carrier,
IF(incoming_or_outgoing_cdr='OUT', term_switch, split(concatenated_outgoing_route_details,'\\|')[5]) as term_switch,
IF(incoming_or_outgoing_cdr='OUT', dir, split(concatenated_outgoing_route_details,'\\|')[6]) as dir,
IF(incoming_or_outgoing_cdr='OUT', type, split(concatenated_outgoing_route_details,'\\|')[7]) as type,
IF(incoming_or_outgoing_cdr='OUT', sub_type, split(concatenated_outgoing_route_details,'\\|')[8]) as sub_type,
IF(incoming_or_outgoing_cdr='OUT', loc_prov, split(concatenated_outgoing_route_details,'\\|')[9]) as loc_prov,
IF(incoming_or_outgoing_cdr='OUT', location, split(concatenated_outgoing_route_details,'\\|')[10]) as location,
IF(incoming_or_outgoing_cdr='OUT', lir, split(concatenated_outgoing_route_details,'\\|')[11]) as lir,
IF(incoming_or_outgoing_cdr='OUT', mar, split(concatenated_outgoing_route_details,'\\|')[12]) as mar,
IF(incoming_or_outgoing_cdr='OUT', ds0, split(concatenated_outgoing_route_details,'\\|')[13]) as ds0,
IF(incoming_or_outgoing_cdr='OUT', route_file_name, split(concatenated_outgoing_route_details,'\\|')[14]) as route_file_name,
IF(incoming_or_outgoing_cdr='OUT', date_loaded, split(concatenated_outgoing_route_details,'\\|')[15]) as date_loaded,
telecom_company_of_calling_pty_ocn_desc as calling_pty_ocn_desc,
telecom_company_of_calling_pty_abbr_ocn_desc as calling_pty_abbr_ocn_desc,
telecom_company_of_calling_pty_ocn_state as calling_pty_ocn_state,
telecom_company_of_calling_pty_category as calling_pty_category,
telecom_company_of_called_pty_ocn_desc as called_pty_ocn_desc,
telecom_company_of_called_pty_abbr_ocn_desc as called_pty_abbr_ocn_desc,
telecom_company_of_called_pty_ocn_state as called_pty_ocn_state,
telecom_company_of_called_pty_category as called_pty_category,
sub_spid_ocn_match,
clng_pty_spid_ocn_match,
cld_pty_spid_ocn_match,
incoming_or_outgoing_cdr,
concatenated_outgoing_route_details,
'OUT' as Direction,
if(clng_pty_ocn is NULL, clng_pty_lrn_spid, if(clng_pty_lrn_spid is NULL, clng_pty_ocn, if(clng_pty_ocn is not NULL and clng_pty_lrn_spid is not NULL and trim(clng_pty_ocn) <> trim(clng_pty_lrn_spid), clng_pty_lrn_spid,  clng_pty_lrn_spid))) as A_OCN_SPID ,
if(cld_pty_ocn is NULL, cld_pty_lrn_spid, if(cld_pty_lrn_spid is NULL, cld_pty_ocn, if(cld_pty_ocn is not NULL and cld_pty_lrn_spid is not NULL and trim(cld_pty_ocn) <> trim(cld_pty_lrn_spid), cld_pty_lrn_spid,  cld_pty_lrn_spid))) as B_OCN_SPID, 
if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 1, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 1, 3),NULL)) as A_NPA,

if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 4, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 4, 3),NULL)) as A_NXX,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 1, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 1, 3),NULL)) as B_NPA,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 4, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 4, 3),NULL)) as B_NXX 

from(
select
cdr.subscriber_no,
cdr.record_sequence_number,
cdr.call_type,
cdr.imsi,
cdr.imei,
cdr.exchange_identity,
cdr.switch_identity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.reg_seizure_charging_start,
cdr.mobile_station_roaming_number,
cdr.redirecting_number,
cdr.cleansed_redirecting_number,
cdr.fault_code,
cdr.eos_info,
cdr.incoming_route_id,
cdr.outgoing_route_id,
cdr.first_cell_id,
cdr.last_cell_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internal_cause_and_loc,
cdr.traffic_activity_code,
cdr.disconnecting_party,
cdr.partial_output_num,
cdr.rco,
cdr.ocn,
cdr.multimediacall,
cdr.teleservicecode,
cdr.tariffclass,
cdr.first_cell_id_extension,
cdr.subscriptiontype,
cdr.srvccindicator,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
cdr.insert_timestamp,
cdr.PD_Subscriber_NO,
cdr.TR_BAN,
cdr.TR_ACCOUNT_SUB_TYPE,
cdr.TR_COMPANY_CODE,
cdr.TR_FRANCHISE_CD,
cdr.TR_PRODUCT_TYPE,
cdr.TR_BA_ACCOUNT_TYPE,
cdr.Sub_OCN,
cdr.Clng_Pty_OCN,
cdr.Cld_Pty_OCN,
cdr.Sub_SPID,
cdr.Clng_Pty_LRN_SPID,
cdr.Cld_Pty_LRN_SPID,
cdr.Dial_Code_Clng_Cld,
cdr.Cnt_name_Clng_Cld,
cdr.call_timestamp_date,

if(cdr.call_type in (2),nvl(out_routes.routeID,in_routes.routeID), if(cdr.call_type in (3, 4, 5), nvl(in_routes.routeID, out_routes.routeID), NULL)) as routeID,
if(cdr.call_type in (2),nvl(out_routes.MSC,in_routes.MSC),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.MSC,out_routes.MSC), NULL)) as MSC,
if(cdr.call_type in (2),nvl(out_routes.MGW,in_routes.MGW),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.MGW,out_routes.MGW), NULL)) as MGW,
if(cdr.call_type in (2),nvl(out_routes.Route_Name,in_routes.Route_Name),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Route_Name,out_routes.Route_Name), NULL)) as Route_Name,
if(cdr.call_type in (2),nvl(out_routes.Carrier,in_routes.Carrier),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Carrier,out_routes.Carrier), NULL)) as Carrier,
if(cdr.call_type in (2),nvl(out_routes.Term_Switch,in_routes.Term_Switch),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Term_Switch,out_routes.Term_Switch), NULL)) as Term_Switch,
if(cdr.call_type in (2),nvl(out_routes.DIR,in_routes.DIR),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.DIR,out_routes.DIR), NULL)) as DIR,
if(cdr.call_type in (2),nvl(out_routes.Type,in_routes.Type),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Type,out_routes.Type), NULL)) as Type,
if(cdr.call_type in (2),nvl(out_routes.Sub_Type,in_routes.Sub_Type),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Sub_Type,out_routes.Sub_Type), NULL)) as Sub_Type,
if(cdr.call_type in (2),nvl(out_routes.Loc_Prov,in_routes.Loc_Prov),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Loc_Prov,out_routes.Loc_Prov), NULL)) as Loc_Prov,
if(cdr.call_type in (2),nvl(out_routes.Location,in_routes.Location),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.Location,out_routes.Location), NULL)) as Location,
if(cdr.call_type in (2),nvl(out_routes.LIR,in_routes.LIR),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.LIR,out_routes.LIR), NULL)) as LIR,
if(cdr.call_type in (2),nvl(out_routes.MAR,in_routes.MAR),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.MAR,out_routes.MAR), NULL)) as MAR,
if(cdr.call_type in (2),nvl(out_routes.DS0,in_routes.DS0),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.DS0,out_routes.DS0), NULL)) as DS0,
if(cdr.call_type in (2),nvl(out_routes.File_Name,in_routes.File_Name),  if(cdr.call_type in (3, 4, 5), nvl(in_routes.File_Name,out_routes.File_Name), NULL)) as Route_File_Name,
if(cdr.call_type in (2),nvl(out_routes.Date_loaded,in_routes.Date_loaded), if(cdr.call_type in (3, 4, 5), nvl(in_routes.Date_loaded,out_routes.Date_loaded), NULL)) as Date_loaded,

if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ocn_desc, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ocn_desc, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ocn_desc,  lerg_calling_spid.ocn_desc))) as telecom_company_of_calling_pty_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ABBR_OCN_DESC, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ABBR_OCN_DESC, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ABBR_OCN_DESC,  lerg_calling_spid.ABBR_OCN_DESC))) as telecom_company_of_calling_pty_ABBR_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.OCN_STATE, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.OCN_STATE, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.OCN_STATE,  lerg_calling_spid.OCN_STATE))) as telecom_company_of_calling_pty_OCN_STATE,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.CATEGORY, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.CATEGORY, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.CATEGORY,  lerg_calling_spid.CATEGORY))) as telecom_company_of_calling_pty_CATEGORY,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ocn_desc, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ocn_desc, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ocn_desc,  lerg_called_spid.ocn_desc))) as telecom_company_of_called_pty_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ABBR_OCN_DESC, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ABBR_OCN_DESC, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ABBR_OCN_DESC,  lerg_called_spid.ABBR_OCN_DESC))) as telecom_company_of_called_pty_ABBR_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.OCN_STATE, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.OCN_STATE, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.OCN_STATE,  lerg_called_spid.OCN_STATE))) as telecom_company_of_called_pty_OCN_STATE,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.CATEGORY, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.CATEGORY, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.CATEGORY,  lerg_called_spid.CATEGORY))) as telecom_company_of_called_pty_CATEGORY,
if(cdr.sub_spid = cdr.sub_ocn, 'Y', 'N') as sub_spid_ocn_match,
if(cdr.clng_pty_lrn_spid = cdr.clng_pty_ocn, 'Y', 'N') as clng_pty_spid_ocn_match,
if(cdr.cld_pty_lrn_spid = cdr.cld_pty_ocn, 'Y', 'N') as cld_pty_spid_ocn_match,

CASE when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH'
when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT'
when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL
ELSE 'IN'
END as incoming_or_outgoing_cdr,

CASE when (case when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH' when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT' when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL ELSE 'IN' END) = 'BOTH' then  
concat_ws('|',out_routes.routeid, out_routes.msc, out_routes.mgw, out_routes.route_name, out_routes.carrier, out_routes.term_switch, out_routes.dir, out_routes.type, out_routes.sub_type,
out_routes.loc_prov, out_routes.location, out_routes.lir, out_routes.mar, out_routes.ds0, out_routes.file_name, out_routes.date_loaded) END as concatenated_outgoing_route_details


from cdrdm.fact_gsm_cdr cdr

left outer join cdrdm.DIM_BK_ROUTES in_routes
ON
(CASE WHEN UPPER(substr(CDR.incoming_route_id, -1)) = 'I' THEN substr(trim(CDR.incoming_route_id), 1, length(trim(CDR.incoming_route_id)) - 1 ) ELSE CDR.incoming_route_id END) = trim(in_routes.Route_Name)
left outer join cdrdm.DIM_BK_ROUTES out_routes
ON
(CASE WHEN UPPER(substr(CDR.outgoing_route_id, -1)) = 'O' THEN substr(trim(CDR.outgoing_route_id), 1, length(trim(CDR.outgoing_route_id)) - 1 ) ELSE CDR.outgoing_route_id END)= trim(out_routes.Route_Name)

Left outer Join (select tt.OCN, tt.OCN_Desc, tt.ABBR_OCN_DESC, tt.OCN_State, tt.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) t from ela_v21.lerg1_ocn) tt where tt.t=1) lerg_called_spid
ON trim(cdr.cld_pty_lrn_spid) = trim(lerg_called_spid.ocn)
Left outer Join (select ss.OCN, ss.OCN_Desc, ss.ABBR_OCN_DESC, ss.OCN_State, ss.Category  from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) s from ela_v21.lerg1_ocn) ss where ss.s=1) lerg_called_ocn
ON trim(cdr.cld_pty_ocn) = trim(lerg_called_ocn.ocn)
Left outer Join (select rr.OCN, rr.OCN_Desc, rr.ABBR_OCN_DESC, rr.OCN_State, rr.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) r from ela_v21.lerg1_ocn) rr where rr.r=1) lerg_calling_spid
ON trim(cdr.clng_pty_lrn_spid) = trim(lerg_calling_spid.ocn)
Left outer Join (select qq.OCN, qq.OCN_Desc, qq.ABBR_OCN_DESC, qq.OCN_State, qq.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) q from ela_v21.lerg1_ocn) qq where qq.q=1) lerg_calling_ocn
ON trim(cdr.clng_pty_ocn) = trim(lerg_calling_ocn.ocn)
where cdr.call_type in (2,3,4,5)
) xxx
where incoming_or_outgoing_cdr in ('OUT', 'BOTH');






create view CDRDM.V_INBOUND_GSM_TRANSIT_CDR as
select
record_sequence_number,
imsi,
exchange_identity,
switch_identity,
call_timestamp,
chargeable_duration,
calling_party_number,
cleansed_calling_number as A_NUMBER,
called_party_number,
cleansed_called_number as B_NUMBER,
original_called_number,
cleansed_original_number,
reg_seizure_charging_start,
redirecting_number,
cleansed_redirecting_number,
fault_code,
eos_info,
incoming_route_id as inbound_route_name,
outgoing_route_id as outbound_route_name,
charged_party,
charged_party_number,
cleansed_charged_number,
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
subscriptiontype,
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
insert_timestamp,
Clng_Pty_OCN,
Cld_Pty_OCN,
Clng_Pty_LRN_SPID,
Cld_Pty_LRN_SPID,
call_timestamp_date,
IF(incoming_or_outgoing_cdr='IN', routeid, split(concatenated_outgoing_route_details,'\\|')[0]) as routeid,
IF(incoming_or_outgoing_cdr='IN', msc, split(concatenated_outgoing_route_details,'\\|')[1]) as msc,
IF(incoming_or_outgoing_cdr='IN', mgw, split(concatenated_outgoing_route_details,'\\|')[2]) as mgw,
IF(incoming_or_outgoing_cdr='IN', route_name, split(concatenated_outgoing_route_details,'\\|')[3]) as route_name,
IF(incoming_or_outgoing_cdr='IN', carrier, split(concatenated_outgoing_route_details,'\\|')[4]) as carrier,
IF(incoming_or_outgoing_cdr='IN', term_switch, split(concatenated_outgoing_route_details,'\\|')[5]) as term_switch,
IF(incoming_or_outgoing_cdr='IN', dir, split(concatenated_outgoing_route_details,'\\|')[6]) as dir,
IF(incoming_or_outgoing_cdr='IN', type, split(concatenated_outgoing_route_details,'\\|')[7]) as type,
IF(incoming_or_outgoing_cdr='IN', sub_type, split(concatenated_outgoing_route_details,'\\|')[8]) as sub_type,
IF(incoming_or_outgoing_cdr='IN', loc_prov, split(concatenated_outgoing_route_details,'\\|')[9]) as loc_prov,
IF(incoming_or_outgoing_cdr='IN', location, split(concatenated_outgoing_route_details,'\\|')[10]) as location,
IF(incoming_or_outgoing_cdr='IN', lir, split(concatenated_outgoing_route_details,'\\|')[11]) as lir,
IF(incoming_or_outgoing_cdr='IN', mar, split(concatenated_outgoing_route_details,'\\|')[12]) as mar,
IF(incoming_or_outgoing_cdr='IN', ds0, split(concatenated_outgoing_route_details,'\\|')[13]) as ds0,
IF(incoming_or_outgoing_cdr='IN', route_file_name, split(concatenated_outgoing_route_details,'\\|')[14]) as route_file_name,
IF(incoming_or_outgoing_cdr='IN', date_loaded, split(concatenated_outgoing_route_details,'\\|')[15]) as date_loaded,
telecom_company_of_calling_pty_ocn_desc as calling_pty_ocn_desc,
telecom_company_of_calling_pty_abbr_ocn_desc as calling_pty_abbr_ocn_desc,
telecom_company_of_calling_pty_ocn_state as calling_pty_ocn_state,
telecom_company_of_calling_pty_category as calling_pty_category,
telecom_company_of_called_pty_ocn_desc as called_pty_ocn_desc,
telecom_company_of_called_pty_abbr_ocn_desc as called_pty_abbr_ocn_desc,
telecom_company_of_called_pty_ocn_state as called_pty_ocn_state,
telecom_company_of_called_pty_category as called_pty_category,
clng_pty_spid_ocn_match,
cld_pty_spid_ocn_match,
incoming_or_outgoing_cdr,
concatenated_outgoing_route_details,
'IN' as Direction,
if(clng_pty_ocn is NULL, clng_pty_lrn_spid, if(clng_pty_lrn_spid is NULL, clng_pty_ocn, if(clng_pty_ocn is not NULL and clng_pty_lrn_spid is not NULL and trim(clng_pty_ocn) <> trim(clng_pty_lrn_spid), clng_pty_lrn_spid,  clng_pty_lrn_spid))) as A_OCN_SPID ,
if(cld_pty_ocn is NULL, cld_pty_lrn_spid, if(cld_pty_lrn_spid is NULL, cld_pty_ocn, if(cld_pty_ocn is not NULL and cld_pty_lrn_spid is not NULL and trim(cld_pty_ocn) <> trim(cld_pty_lrn_spid), cld_pty_lrn_spid,  cld_pty_lrn_spid))) as B_OCN_SPID, 
if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 1, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 1, 3),NULL)) as A_NPA,

if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 4, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 4, 3),NULL)) as A_NXX,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 1, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 1, 3),NULL)) as B_NPA,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 4, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 4, 3),NULL)) as B_NXX 

FROM(
select
cdr.record_sequence_number,
cdr.imsi,
cdr.exchange_identity,
cdr.switch_identity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.reg_seizure_charging_start,
cdr.redirecting_number,
cdr.cleansed_redirecting_number,
cdr.fault_code,
cdr.eos_info,
cdr.incoming_route_id,
cdr.outgoing_route_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internal_cause_and_loc,
cdr.traffic_activity_code,
cdr.disconnecting_party,
cdr.subscriptiontype,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
cdr.insert_timestamp,
cdr.Clng_Pty_OCN,
cdr.Cld_Pty_OCN,
cdr.Clng_Pty_LRN_SPID,
cdr.Cld_Pty_LRN_SPID,
cdr.call_timestamp_date,

nvl(out_routes.routeID, in_routes.routeID) as routeID,
nvl(out_routes.MSC, in_routes.MSC) as MSC,
nvl(out_routes.MGW, in_routes.MGW) as MGW,
nvl(out_routes.Route_Name, in_routes.Route_Name) as Route_Name,
nvl(out_routes.Carrier, in_routes.Carrier) as Carrier,
nvl(out_routes.Term_Switch, in_routes.Term_Switch) as Term_Switch,
nvl(out_routes.DIR, in_routes.DIR) as DIR,
nvl(out_routes.Type, in_routes.Type) as Type,
nvl(out_routes.Sub_Type, in_routes.Sub_Type) as Sub_Type,
nvl(out_routes.Loc_Prov, in_routes.Loc_Prov) as Loc_Prov,
nvl(out_routes.Location, in_routes.Location) as Location,
nvl(out_routes.LIR, in_routes.LIR) as LIR,
nvl(out_routes.MAR, in_routes.MAR) as MAR,
nvl(out_routes.DS0, in_routes.DS0) as DS0,
nvl(out_routes.File_Name, in_routes.File_Name) as Route_File_Name,
nvl(out_routes.Date_loaded, in_routes.Date_loaded) as Date_loaded,

if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ocn_desc, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ocn_desc, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ocn_desc,  lerg_calling_spid.ocn_desc))) as telecom_company_of_calling_pty_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ABBR_OCN_DESC, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ABBR_OCN_DESC, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ABBR_OCN_DESC,  lerg_calling_spid.ABBR_OCN_DESC))) as telecom_company_of_calling_pty_ABBR_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.OCN_STATE, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.OCN_STATE, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.OCN_STATE,  lerg_calling_spid.OCN_STATE))) as telecom_company_of_calling_pty_OCN_STATE,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.CATEGORY, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.CATEGORY, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.CATEGORY,  lerg_calling_spid.CATEGORY))) as telecom_company_of_calling_pty_CATEGORY,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ocn_desc, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ocn_desc, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ocn_desc,  lerg_called_spid.ocn_desc))) as telecom_company_of_called_pty_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ABBR_OCN_DESC, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ABBR_OCN_DESC, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ABBR_OCN_DESC,  lerg_called_spid.ABBR_OCN_DESC))) as telecom_company_of_called_pty_ABBR_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.OCN_STATE, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.OCN_STATE, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.OCN_STATE,  lerg_called_spid.OCN_STATE))) as telecom_company_of_called_pty_OCN_STATE,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.CATEGORY, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.CATEGORY, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.CATEGORY,  lerg_called_spid.CATEGORY))) as telecom_company_of_called_pty_CATEGORY,
if(cdr.clng_pty_lrn_spid = cdr.clng_pty_ocn, 'Y', 'N') as clng_pty_spid_ocn_match,
if(cdr.cld_pty_lrn_spid = cdr.cld_pty_ocn, 'Y', 'N') as cld_pty_spid_ocn_match,

CASE when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH'
when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT'
when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL
ELSE 'IN'
END as incoming_or_outgoing_cdr,

CASE when (case when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH' when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT' when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL ELSE 'IN' END) = 'BOTH' then  
concat_ws('|',in_routes.routeid, in_routes.msc, in_routes.mgw, in_routes.route_name, in_routes.carrier, in_routes.term_switch, in_routes.dir, in_routes.type, in_routes.sub_type,
in_routes.loc_prov, in_routes.location, in_routes.lir, in_routes.mar, in_routes.ds0, in_routes.file_name, in_routes.date_loaded) END as concatenated_outgoing_route_details


from cdrdm.fact_gsm_transit_cdr cdr

left outer join cdrdm.DIM_BK_ROUTES in_routes
ON
(CASE WHEN UPPER(substr(CDR.incoming_route_id, -1)) = 'I' THEN substr(trim(CDR.incoming_route_id), 1, length(trim(CDR.incoming_route_id)) - 1 ) ELSE CDR.incoming_route_id END) = trim(in_routes.Route_Name)
left outer join cdrdm.DIM_BK_ROUTES out_routes
ON
(CASE WHEN UPPER(substr(CDR.outgoing_route_id, -1)) = 'O' THEN substr(trim(CDR.outgoing_route_id), 1, length(trim(CDR.outgoing_route_id)) - 1 ) ELSE CDR.outgoing_route_id END)= trim(out_routes.Route_Name)


Left outer Join (select tt.OCN, tt.OCN_Desc, tt.ABBR_OCN_DESC, tt.OCN_State, tt.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) t from ela_v21.lerg1_ocn) tt where tt.t=1) lerg_called_spid
ON trim(cdr.cld_pty_lrn_spid) = trim(lerg_called_spid.ocn)
Left outer Join (select ss.OCN, ss.OCN_Desc, ss.ABBR_OCN_DESC, ss.OCN_State, ss.Category  from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) s from ela_v21.lerg1_ocn) ss where ss.s=1) lerg_called_ocn
ON trim(cdr.cld_pty_ocn) = trim(lerg_called_ocn.ocn)
Left outer Join (select rr.OCN, rr.OCN_Desc, rr.ABBR_OCN_DESC, rr.OCN_State, rr.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) r from ela_v21.lerg1_ocn) rr where rr.r=1) lerg_calling_spid
ON trim(cdr.clng_pty_lrn_spid) = trim(lerg_calling_spid.ocn)
Left outer Join (select qq.OCN, qq.OCN_Desc, qq.ABBR_OCN_DESC, qq.OCN_State, qq.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) q from ela_v21.lerg1_ocn) qq where qq.q=1) lerg_calling_ocn
ON trim(cdr.clng_pty_ocn) = trim(lerg_calling_ocn.ocn)
) xxx
where incoming_or_outgoing_cdr in ('IN', 'BOTH');




create view CDRDM.V_OUTBOUND_GSM_TRANSIT_CDR as
select
record_sequence_number,
imsi,
exchange_identity,
switch_identity,
call_timestamp,
chargeable_duration,
calling_party_number,
cleansed_calling_number as A_NUMBER,
called_party_number,
cleansed_called_number as B_NUMBER,
original_called_number,
cleansed_original_number,
reg_seizure_charging_start,
redirecting_number,
cleansed_redirecting_number,
fault_code,
eos_info,
incoming_route_id as inbound_route_name,
outgoing_route_id as outbound_route_name,
charged_party,
charged_party_number,
cleansed_charged_number,
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
subscriptiontype,
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
insert_timestamp,
Clng_Pty_OCN,
Cld_Pty_OCN,
Clng_Pty_LRN_SPID,
Cld_Pty_LRN_SPID,
call_timestamp_date,
routeID,
msc,
mgw,
route_name,
carrier,
term_switch,
dir,
type,
sub_type,
loc_prov,
location,
lir,
mar,
ds0,
route_file_name,
date_loaded,
telecom_company_of_calling_pty_ocn_desc as calling_pty_ocn_desc,
telecom_company_of_calling_pty_abbr_ocn_desc as calling_pty_abbr_ocn_desc,
telecom_company_of_calling_pty_ocn_state as calling_pty_ocn_state,
telecom_company_of_calling_pty_category as calling_pty_category,
telecom_company_of_called_pty_ocn_desc as called_pty_ocn_desc,
telecom_company_of_called_pty_abbr_ocn_desc as called_pty_abbr_ocn_desc,
telecom_company_of_called_pty_ocn_state as called_pty_ocn_state,
telecom_company_of_called_pty_category as called_pty_category,
clng_pty_spid_ocn_match,
cld_pty_spid_ocn_match,
incoming_or_outgoing_cdr,
concatenated_outgoing_route_details,
'OUT' as Direction,
if(clng_pty_ocn is NULL, clng_pty_lrn_spid, if(clng_pty_lrn_spid is NULL, clng_pty_ocn, if(clng_pty_ocn is not NULL and clng_pty_lrn_spid is not NULL and trim(clng_pty_ocn) <> trim(clng_pty_lrn_spid), clng_pty_lrn_spid,  clng_pty_lrn_spid))) as A_OCN_SPID ,
if(cld_pty_ocn is NULL, cld_pty_lrn_spid, if(cld_pty_lrn_spid is NULL, cld_pty_ocn, if(cld_pty_ocn is not NULL and cld_pty_lrn_spid is not NULL and trim(cld_pty_ocn) <> trim(cld_pty_lrn_spid), cld_pty_lrn_spid,  cld_pty_lrn_spid))) as B_OCN_SPID, 
if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 1, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 1, 3),NULL)) as A_NPA,

if(length(cleansed_calling_number)=10,substr(cast(cleansed_calling_number as string), 4, 3),if(length(cleansed_calling_number)>10,
substr(substr(cast(cleansed_calling_number as string),length(cleansed_calling_number) - 9, length(cleansed_calling_number)), 4, 3),NULL)) as A_NXX,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 1, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 1, 3),NULL)) as B_NPA,

if(length(cleansed_called_number)=10,substr(cast(cleansed_called_number as string), 4, 3),if(length(cleansed_called_number)>10,
substr(substr(cast(cleansed_called_number as string),length(cleansed_called_number) - 9, length(cleansed_called_number)), 4, 3),NULL)) as B_NXX 

FROM(
select
cdr.record_sequence_number,
cdr.imsi,
cdr.exchange_identity,
cdr.switch_identity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.reg_seizure_charging_start,
cdr.redirecting_number,
cdr.cleansed_redirecting_number,
cdr.fault_code,
cdr.eos_info,
cdr.incoming_route_id,
cdr.outgoing_route_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internal_cause_and_loc,
cdr.traffic_activity_code,
cdr.disconnecting_party,
cdr.subscriptiontype,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
cdr.insert_timestamp,
cdr.Clng_Pty_OCN,
cdr.Cld_Pty_OCN,
cdr.Clng_Pty_LRN_SPID,
cdr.Cld_Pty_LRN_SPID,
cdr.call_timestamp_date,

nvl(out_routes.routeID, in_routes.routeID) as routeID,
nvl(out_routes.MSC, in_routes.MSC) as MSC,
nvl(out_routes.MGW, in_routes.MGW) as MGW,
nvl(out_routes.Route_Name, in_routes.Route_Name) as Route_Name,
nvl(out_routes.Carrier, in_routes.Carrier) as Carrier,
nvl(out_routes.Term_Switch, in_routes.Term_Switch) as Term_Switch,
nvl(out_routes.DIR, in_routes.DIR) as DIR,
nvl(out_routes.Type, in_routes.Type) as Type,
nvl(out_routes.Sub_Type, in_routes.Sub_Type) as Sub_Type,
nvl(out_routes.Loc_Prov, in_routes.Loc_Prov) as Loc_Prov,
nvl(out_routes.Location, in_routes.Location) as Location,
nvl(out_routes.LIR, in_routes.LIR) as LIR,
nvl(out_routes.MAR, in_routes.MAR) as MAR,
nvl(out_routes.DS0, in_routes.DS0) as DS0,
nvl(out_routes.File_Name, in_routes.File_Name) as Route_File_Name,
nvl(out_routes.Date_loaded, in_routes.Date_loaded) as Date_loaded,

if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ocn_desc, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ocn_desc, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ocn_desc,  lerg_calling_spid.ocn_desc))) as telecom_company_of_calling_pty_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.ABBR_OCN_DESC, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.ABBR_OCN_DESC, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.ABBR_OCN_DESC,  lerg_calling_spid.ABBR_OCN_DESC))) as telecom_company_of_calling_pty_ABBR_OCN_DESC,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.OCN_STATE, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.OCN_STATE, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.OCN_STATE,  lerg_calling_spid.OCN_STATE))) as telecom_company_of_calling_pty_OCN_STATE,
if(cdr.clng_pty_ocn is NULL, lerg_calling_spid.CATEGORY, if(cdr.clng_pty_lrn_spid is NULL, lerg_calling_ocn.CATEGORY, if(cdr.clng_pty_ocn is not NULL and cdr.clng_pty_lrn_spid is not NULL and trim(cdr.clng_pty_ocn) <> trim(cdr.clng_pty_lrn_spid), lerg_calling_spid.CATEGORY,  lerg_calling_spid.CATEGORY))) as telecom_company_of_calling_pty_CATEGORY,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ocn_desc, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ocn_desc, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ocn_desc,  lerg_called_spid.ocn_desc))) as telecom_company_of_called_pty_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.ABBR_OCN_DESC, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.ABBR_OCN_DESC, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.ABBR_OCN_DESC,  lerg_called_spid.ABBR_OCN_DESC))) as telecom_company_of_called_pty_ABBR_OCN_DESC,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.OCN_STATE, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.OCN_STATE, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.OCN_STATE,  lerg_called_spid.OCN_STATE))) as telecom_company_of_called_pty_OCN_STATE,
if(cdr.cld_pty_ocn is NULL, lerg_called_spid.CATEGORY, if(cdr.cld_pty_lrn_spid is NULL, lerg_called_ocn.CATEGORY, if(cdr.cld_pty_ocn is not NULL and cdr.cld_pty_lrn_spid is not NULL and trim(cdr.cld_pty_ocn) <> trim(cdr.cld_pty_lrn_spid), lerg_called_spid.CATEGORY,  lerg_called_spid.CATEGORY))) as telecom_company_of_called_pty_CATEGORY,
if(cdr.clng_pty_lrn_spid = cdr.clng_pty_ocn, 'Y', 'N') as clng_pty_spid_ocn_match,
if(cdr.cld_pty_lrn_spid = cdr.cld_pty_ocn, 'Y', 'N') as cld_pty_spid_ocn_match,

CASE when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH'
when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT'
when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL
ELSE 'IN'
END as incoming_or_outgoing_cdr,

CASE when (case when in_routes.route_name is not NULL and out_routes.route_name is not NULL then 'BOTH' when in_routes.route_name is NULL and out_routes.route_name is not NULL then 'OUT' when in_routes.route_name is NULL and out_routes.route_name is NULL then NULL ELSE 'IN' END) = 'BOTH' then  
concat_ws('|',in_routes.routeid, in_routes.msc, in_routes.mgw, in_routes.route_name, in_routes.carrier, in_routes.term_switch, in_routes.dir, in_routes.type, in_routes.sub_type,
in_routes.loc_prov, in_routes.location, in_routes.lir, in_routes.mar, in_routes.ds0, in_routes.file_name, in_routes.date_loaded) END as concatenated_outgoing_route_details


from cdrdm.fact_gsm_transit_cdr cdr

left outer join cdrdm.DIM_BK_ROUTES in_routes
ON
(CASE WHEN UPPER(substr(CDR.incoming_route_id, -1)) = 'I' THEN substr(trim(CDR.incoming_route_id), 1, length(trim(CDR.incoming_route_id)) - 1 ) ELSE CDR.incoming_route_id END) = trim(in_routes.Route_Name)
left outer join cdrdm.DIM_BK_ROUTES out_routes
ON
(CASE WHEN UPPER(substr(CDR.outgoing_route_id, -1)) = 'O' THEN substr(trim(CDR.outgoing_route_id), 1, length(trim(CDR.outgoing_route_id)) - 1 ) ELSE CDR.outgoing_route_id END)= trim(out_routes.Route_Name)


Left outer Join (select tt.OCN, tt.OCN_Desc, tt.ABBR_OCN_DESC, tt.OCN_State, tt.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) t from ela_v21.lerg1_ocn) tt where tt.t=1) lerg_called_spid
ON trim(cdr.cld_pty_lrn_spid) = trim(lerg_called_spid.ocn)
Left outer Join (select ss.OCN, ss.OCN_Desc, ss.ABBR_OCN_DESC, ss.OCN_State, ss.Category  from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) s from ela_v21.lerg1_ocn) ss where ss.s=1) lerg_called_ocn
ON trim(cdr.cld_pty_ocn) = trim(lerg_called_ocn.ocn)
Left outer Join (select rr.OCN, rr.OCN_Desc, rr.ABBR_OCN_DESC, rr.OCN_State, rr.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) r from ela_v21.lerg1_ocn) rr where rr.r=1) lerg_calling_spid
ON trim(cdr.clng_pty_lrn_spid) = trim(lerg_calling_spid.ocn)
Left outer Join (select qq.OCN, qq.OCN_Desc, qq.ABBR_OCN_DESC, qq.OCN_State, qq.Category from  (select OCN, OCN_Desc, ABBR_OCN_DESC, OCN_State, Category, row_number() over (partition by ocn order by gg_commit_ts desc) q from ela_v21.lerg1_ocn) qq where qq.q=1) lerg_calling_ocn
ON trim(cdr.clng_pty_ocn) = trim(lerg_calling_ocn.ocn)
) xxx
where incoming_or_outgoing_cdr in ('OUT', 'BOTH');


