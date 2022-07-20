CREATE OR REPLACE VIEW HEM.SUBSCRIBER_MODEM_DIM_VW AS
Select
TopoDim.cbu as cbu_code ,
TopoDim.phub as primary_hub ,
TopoDim.rtn_seg as secondary_hub ,
SubMdmDim.smt ,
SubMdmDim.postal_zip_code ,
SubMdmDim.full_acct_num ,
SubMdmDim.company_number ,
SubMdmDim.account_number ,
SubMdmDim.dtv_model_num ,
SubMdmDim.dtv_serial_num ,
SubMdmDim.billing_src_mac_id ,
SubMdmDim.equipment_status_code ,
SubMdmDim.municipality ,
SubMdmDim.manufacturer_make ,
SubMdmDim.manufacturer_model ,
SubMdmDim.product_code ,
SubMdmDim.service_product_code ,
SubMdmDim.system ,
SubMdmDim.cm_mac_addr ,
SubMdmDim.technology ,
SubMdmDim.ds_tuners_count ,
SubMdmDim.us_tuners_count
from
(
Select * from
(
select
Sub.*,
row_number() over (partition by cm_mac_addr order by eff_start_dt desc) as mdmRnk
from hem.Subscriber_modem_dim as sub
where crnt_f= 'Y'
) as mdm
where mdmRnk = 1
) as SubMdmDim left join
(
Select * from
(
select topology.*,
row_number() over (partition by sam_key order by src_effective_date desc) as topoRnk
from hem.rhsi_topology_dim as topology
where crnt_flg = 'Y'
) as topo
where topoRnk = 1
) as TopoDim
On (SubMdmDim.Sam_key = TopoDim.Sam_key)
