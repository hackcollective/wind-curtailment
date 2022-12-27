select level_fpn                         as level_fpn_mw,
       level_after_boal                  as level_after_boal_mw,
       delta_mw,
       level_fpn_mwh,
       level_fpn * 0.5                   as level_fpn_mwh,
       level_after_boal * 0.5            as level_after_boal_mwh,
       system_buy_price,
       cost_gbp,
       system_buy_price * delta_mw * 0.5 as turnup_cost_gbp
from curtailment c
         join sbp s on c.time = s.time
where time BETWEEN :start_time AND :end_time
order by time