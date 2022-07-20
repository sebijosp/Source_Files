Insert OVERWRITE TABLE cdrdm.bnk_lerg1_ocn_tmp
   select a.ocn,a.ocn_desc
   from 
   (select OCN,ocn_desc,row_number() over (partition by OCN order by ocn_desc) as first_ocn_occurrence from ela_v21.lerg1_ocn  )a 
where a.first_ocn_occurrence = 1 ;
