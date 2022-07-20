load data local inpath '${hiveconf:DIR}/LTE_Unweighted_Availability_M.CSV'  overwrite into table cdrdm.LTE_Unweighted_Unavailability_monthly;

