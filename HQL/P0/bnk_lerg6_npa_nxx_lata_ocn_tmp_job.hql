Insert OVERWRITE TABLE cdrdm.bnk_lerg6_npa_nxx_lata_ocn_tmp
 Select z.NPA as NPA,z.NXX as NXX,z.lata as lata, z.OCN as OCN
from 
(SELECT NPA, NXX, OCN,lata, row_number() over (partition by NPA, NXX order by OCN,lata) as first_NPA_NPX_occurrence 
from ela_v21.lerg6_npa_nxx_blocks) z 
where z.first_NPA_NPX_occurrence = 1;
