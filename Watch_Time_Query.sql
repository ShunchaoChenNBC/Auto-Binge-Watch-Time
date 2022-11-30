
with cte as (select
adobe_timestamp,
lower(b.Vdo_Titles) as Vdo_Titles,
case when Video_Start_Type = 'Auto-Play' and Next_Actions = 'Vdo_End' then Next_Timestamp - adobe_timestamp end as Auto_Binge_Watch_Time,
case when Video_Start_Type = 'Up-Next' and Next_Actions = 'Vdo_End' then Next_Timestamp - adobe_timestamp end as Up_Next_Watch_Time,
from
(SELECT a.*,
lead(Video_Start_Type) over (partition by lower(Vdo_Titles) order by adobe_timestamp) as Next_Actions,
lead(adobe_timestamp) over (partition by lower(Vdo_Titles) order by adobe_timestamp) as Next_Timestamp,
FROM `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Auto-Binge-Sample` a
where Video_Start_Type is not null
order by adobe_timestamp) b),
cte2 as (select 
adobe_timestamp,
Vdo_Titles,
extract(day from Auto_Binge_Watch_Time)*24*60*60+extract(hour from Auto_Binge_Watch_Time)*60*60+extract(minute from Auto_Binge_Watch_Time)*60+extract(second from Auto_Binge_Watch_Time) as Auto_Binge_Watch_Time,
extract(day from Up_Next_Watch_Time)*24*60*60+extract(hour from Up_Next_Watch_Time)*60*60+extract(minute from Up_Next_Watch_Time)*60+extract(second from Up_Next_Watch_Time) as Up_Next_Watch_Time,
from cte)


select
Auto_Binge_Watch_Time,
Up_Next_Watch_Time,
Total_Watch_Time,
round(Up_Next_Watch_Time/Total_Watch_Time,2) as Prc_Up_Next,
round(Auto_Binge_Watch_Time/Total_Watch_Time,2) as Prc_Auto_Binge
from (select 
sum(Auto_Binge_Watch_Time) as Auto_Binge_Watch_Time,
sum(Up_Next_Watch_Time) as Up_Next_Watch_Time,
extract(day from (max(adobe_timestamp)-min(adobe_timestamp)))*24*60*60\
+extract(hour from (max(adobe_timestamp)-min(adobe_timestamp)))*60*60\
+extract(minute from (max(adobe_timestamp)-min(adobe_timestamp)))*60\
+extract(second from (max(adobe_timestamp)-min(adobe_timestamp))) as Total_Watch_Time
from cte2) c



