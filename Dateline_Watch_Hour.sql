with cte as (select
Adobe_Date,
Adobe_Tracking_ID,
adobe_timestamp,
lower(b.Display_Name) as Vdo_Titles,
case when Last_Actions = 'Auto-Play' and Video_Start_Type = 'Vdo_End' then num_seconds_played_no_ads/3600 end as Auto_Binge_Hours,
case when Last_Actions = 'Clicked-Up-Next' and Video_Start_Type = 'Vdo_End' then num_seconds_played_no_ads/3600 end as Up_Next_Hours,
num_seconds_played_no_ads/3600 as Watch_Hours
from
(SELECT a.*,
lag(Video_Start_Type) over (partition by Adobe_Tracking_ID order by adobe_timestamp) as Last_Actions
FROM `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Dateline_20_Says` a
where Adobe_Tracking_ID is not null and Video_Start_Type is not null
order by Adobe_Tracking_ID, adobe_timestamp) b
where b.Display_Name is not null)

select *
from 
(select 
Adobe_Date,
round(sum(Auto_Binge_Hours),4) as Grand_Auto_Binge_Hours,
round(sum(Up_Next_Hours),4) as Grand_Up_Next_Hours,
round(sum (Watch_Hours),4) as Grand_Total_Watch_Hours,
round(SAFE_DIVIDE(sum(Auto_Binge_Hours),sum (Watch_Hours)),4) as Prc_Auto_Binge,
round(safe_divide(sum(Up_Next_Hours), sum (Watch_Hours)),4) as Prc_Up_Next
from cte
group by 1
) as cte1
order by 1
