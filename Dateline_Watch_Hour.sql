with cte as (select
Adobe_Date,
Adobe_Tracking_ID,
adobe_timestamp,
lower(b.Display_Name) as Vdo_Titles,
Feeder_Video,
case when Last_Actions = 'Auto-Play' and Video_Start_Type = 'Vdo_End' then num_seconds_played_no_ads/3600 end as Auto_Binge_Hours,
case when Last_Actions = 'Auto-Play' and Video_Start_Type = 'Vdo_End' then num_seconds_played_no_ads/3600 
     when Feeder_video = b.Display_Name and Video_Start_Type = 'Vdo_End' then  num_seconds_played_no_ads/3600 end as New_Auto_Binge_Hours, -- new_logic
case when Last_Actions = 'Clicked-Up-Next' and Video_Start_Type = 'Vdo_End' then num_seconds_played_no_ads/3600 end as Up_Next_Hours,
num_seconds_played_no_ads/3600 as Watch_Hours
from
(SELECT a.*,
lag(Video_Start_Type) over (partition by Adobe_Tracking_ID,adobe_date order by adobe_timestamp) as Last_Actions
FROM `nbcu-ds-sandbox-a-001.Shunchao_Sandbox_Final.Dateline_Nov_30_Days` a
where Adobe_Tracking_ID is not null and Video_Start_Type is not null
order by Adobe_Tracking_ID, adobe_timestamp) b
where b.Display_Name is not null)

select 
Adobe_Date,
lower(Feeder_Video) as Feeder_Video,
round(sum(Auto_Binge_Hours),4) as Grand_Auto_Binge_Hours,
round(sum(New_Auto_Binge_Hours),4) as Grand_Auto_Binge_New,
round(sum(Up_Next_Hours),4) as Grand_Up_Next_Hours,
round(sum (Watch_Hours),4) as Grand_Total_Watch_Hours,
round(SAFE_DIVIDE(sum(New_Auto_Binge_Hours),sum (Watch_Hours)),4) as Prc_Auto_Binge_New,
round(SAFE_DIVIDE(sum(Auto_Binge_Hours),sum (Watch_Hours)),4) as Prc_Auto_Binge,
round(safe_divide(sum(Up_Next_Hours), sum (Watch_Hours)),4) as Prc_Up_Next
from cte
group by 1,2
order by 1,6 desc
