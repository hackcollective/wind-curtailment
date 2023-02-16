select *, delta_mw*1e-3 as delta_gw
from curtailment
where delta_mw = (select max(delta_mw) from curtailment)
