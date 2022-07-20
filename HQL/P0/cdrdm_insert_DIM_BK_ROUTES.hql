SET hive.variable.substitute=true;
SET hive.default.fileformat=orc;
SET hive.execution.engine=tez;

use cdrdm; 
Drop table cdrdm.DIM_BK_Routes;

CREATE TABLE cdrdm.DIM_BK_Routes( 
RouteID Varchar(10),
MSC Varchar(20),
MGW Varchar(50),
Route_Name Varchar(100),
Carrier Varchar(100),
Term_Switch Varchar(100),
DIR Varchar(20),
Type Varchar(20),
Sub_Type Varchar(20),
Loc_Prov Varchar(50),
Location Varchar(100),
LIR Varchar(100),
MAR Varchar(100),
DS0 Varchar(50),
File_Name Varchar(50),
Date_loaded string)
stored as orc;

insert overwrite table cdrdm.DIM_BK_Routes 
Select lBK.*, 
'${hiveconf:File_Name}' as File_Name, 
from_unixtime(unix_timestamp()) as Date_loaded 
from ext_cdrdm.landing_BK_Routes lBK;
