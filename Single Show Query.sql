

with cte as (select b.*,
sum(case when b.Feeder_Video = b.Display_Name then b.num_seconds_played_no_ads else 0 end) over (partition by Adobe_Tracking_ID, Adobe_Date, grp) as Episode_Time
from
(SELECT a.*,
lag(Video_Start_Type) over (partition by Adobe_Tracking_ID,adobe_date order by adobe_timestamp) as Last_Actions,
sum(case when Feeder_Video = Display_Name then 0 else 1 end) over (partition by Adobe_Tracking_ID, Adobe_Date order by Adobe_Timestamp) as grp
FROM 
`nbcu-ds-sandbox-a-001.Shunchao_Sandbox_Final.A_Friend_Of_Family_Oct_New` a) b),

cte1 as 
(select 
Adobe_Tracking_ID,
Adobe_Date,
Adobe_Timestamp,
Last_Actions,
case when Feeder_Video is null and num_seconds_played_no_ads is not null then "Manual-Selection" 
when lower(Feeder_Video) like '%trailer%' then "Manual-Selection" -- all trailers are manual
when Last_Actions like '%Manual%' and Video_Start_Type = 'Vdo_End' then "Manual-Selection"
when Last_Actions = 'Auto-Play' and Video_Start_Type = 'Vdo_End'and Feeder_Video not like "%trailer%" then "Auto-Play" 
when Last_Actions = 'Clicked-Up-Next' and Video_Start_Type = 'Vdo_End'and Feeder_Video not like "%trailer%" then "Clicked-Up-Next" 
when Feeder_Video <> Display_Name and Episode_Time > 0 and num_seconds_played_no_ads is not null then "Auto-Play"-- episode attribution
when Feeder_Video <> Display_Name and Feeder_Video != "" and Episode_Time = 0 and num_seconds_played_no_ads <= 30 then "Auto-Play"--only watch one show and watch less than 30s
when Feeder_Video <> Display_Name and Feeder_Video != "" and Episode_Time = 0 and num_seconds_played_no_ads > 30 then "Unattributed" -- only watch one show and watch more than 30s
when Video_Start_Type = "Auto-Play" and (Feeder_Video is null or Feeder_Video = "") then "Manual-Selection" -- if cue-up auto but no feeder videos put it to Manual-Selection
else Video_Start_Type
end as Video_Start_Type,
device_name,
Feeder_Video,
Feeder_Video_Id,
Display_Name,
video_id,
num_seconds_played_no_ads,
grp,
Episode_Time,
case when Feeder_Video is null or Feeder_Video <> Display_Name then ifnull(num_seconds_played_no_ads,0) + Episode_Time else 0 end as New_Watch_Time
from cte
order by 1,2,3)

select *
from cte1
order by 1,2,3

-- select cte2.*,
-- sum(cte2.New_Watch_Time) over (partition by Adobe_Date) as Watch_Total
-- from 
-- (select 
-- Adobe_Date,
-- Video_Start_Type,
-- count(distinct Adobe_Tracking_ID) as Unique_Accounts,
-- round(sum(New_Watch_Time)/3600,2) as New_Watch_Time,
-- from cte1
-- group by 1,2) cte2
-- order by 1,2,4 desc
