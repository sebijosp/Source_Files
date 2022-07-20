Create table EXT_CDRDM.LANDING_BK_Routes (
RouteID	Varchar(10),
MSC	Varchar(20),
MGW	Varchar(50),
Route_Name	Varchar(100),
Carrier	Varchar(100),
Term_Switch	Varchar(100),
DIR	Varchar(20),
Type	Varchar(20),
Sub_Type	Varchar(20),
Loc_Prov	Varchar(50),
Location	Varchar(100),
LIR	Varchar(100),
MAR	Varchar(100),
DS0	Varchar(50)
)
COMMENT 'BK Route data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/userapps/cdrdm/landing/CDR_ROUTE';
