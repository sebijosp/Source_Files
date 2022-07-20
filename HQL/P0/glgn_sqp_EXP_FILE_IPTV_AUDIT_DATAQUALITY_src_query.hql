 select

<SELECT_COLS_PLACEHOLDER>

from iptv.audit_dataquality t
where  t.EVENT_DATE >= date_add(current_date,-7) 
