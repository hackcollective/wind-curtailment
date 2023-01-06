--> 1. Joins in the `sbp` table with 'curtailment'
--> 2. Manipulates some columns which used to be manipulated in pandas (faster in SQL)
--> 3. start and end time are parameters which are interpolated by the SQL engine
--> Note a fixed gass turn up price is used #52

select c.time                            as time,
       level_fpn                         as level_fpn_mw,
       level_after_boal                  as level_after_boal_mw,
       delta_mw,
       level_fpn * 0.5                   as level_fpn_mwh,
       level_after_boal * 0.5            as level_after_boal_mwh,
-->        system_buy_price,
       cost_gbp,
-->    system_buy_price * delta_mw * 0.5 as turnup_cost_gbp
       200 * delta_mw * 0.5 as turnup_cost_gbp
from curtailment c
         --> join sbp s on c.time = s.time
where c.time BETWEEN CAST(%(start_time)s as TIMESTAMP)
    AND CAST(%(end_time)s as TIMESTAMP)
order by c.time