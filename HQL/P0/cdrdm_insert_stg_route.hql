set hive.execution.engine=tez;
DROP TABLE IF EXISTS CDRDM.STG_ROUTE;
CREATE TABLE CDRDM.STG_ROUTE
AS SELECT rt.SWITCH_ID, rt.ROUTE_ID,rt.SERVE_SID, case when Rt.switch_id = 'LTE' then lpad(Rt.ROUTE_ID,8,'0') when Rt.switch_id = '3G' AND  CAST(Rt.ROUTE_ID AS INT) IS NOT NULL then lpad(conv(Rt.ROUTE_ID,10,16),8,'0') ELSE Rt.ROUTE_ID end AS ROUTE_ID_HEX
FROM ela_v21.route_gg rt
WHERE rt.serve_sID = '17214';
