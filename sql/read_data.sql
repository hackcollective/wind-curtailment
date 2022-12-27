select  level_fpn as level_fpn_mw,
        level_after_boal as level_after_boal_mw,
        delta_mw,
        cost_gbp,
        system_buy_price
from curtailment c join sbp s on c.time = s.time
where time BETWEEN :start_time AND :end_time
order by time