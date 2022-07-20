Insert OVERWRITE TABLE cdrdm.bnk_lsms_tn_ocn_desc
select z.tn,z.spid,a.ocn_desc from
(SELECT spid,tn
from ela_v21.lsms_replica_b) z
left outer join
cdrdm.bnk_lerg1_ocn_tmp a
on z.spid = a.ocn;
