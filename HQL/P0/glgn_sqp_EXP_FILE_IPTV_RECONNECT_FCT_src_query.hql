select
<SELECT_COLS_PLACEHOLDER>
from iptv.Reconnect_fct
where  Event_Date >= date_add(current_date,-30)
and <SQP_DELTA_WHERE_CONDITION_PLACEHOLDER>
