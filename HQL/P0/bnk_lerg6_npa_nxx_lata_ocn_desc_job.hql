Insert OVERWRITE TABLE `cdrdm.bnk_lerg6_npa_nxx_lata_ocn_desc`
     Select z.npa,z.nxx,z.lata,z.ocn,a.ocn_desc
from 
cdrdm.bnk_lerg6_npa_nxx_lata_ocn_tmp z 
left outer join cdrdm.bnk_lerg1_ocn_tmp a 
on z.ocn = a.ocn;
