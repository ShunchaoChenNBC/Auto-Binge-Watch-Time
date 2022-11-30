

with cte as (select
Adobe_Tracking_ID,
adobe_timestamp,
lower(b.Display_Name) as Vdo_Titles,
case when Video_Start_Type = 'Auto-Play' and Next_Actions = 'Vdo_End' then Next_Timestamp - adobe_timestamp end as Auto_Binge_Watch_Time,
case when Video_Start_Type = 'Up-Next' and Next_Actions = 'Vdo_End' then Next_Timestamp - adobe_timestamp end as Up_Next_Watch_Time,
case when Video_Start_Type = 'Auto-Play' and Next_Actions = 'Vdo_End' then num_seconds_played_no_ads/3600 end as Auto_Binge_No_Ad_Hours,
case when Video_Start_Type = 'Up-Next' and Next_Actions = 'Vdo_End' then num_seconds_played_no_ads/3600 end as Up_Next_No_Ad_Hours,
num_seconds_played_no_ads
from
(SELECT a.*,
lead(Video_Start_Type) over (partition by Adobe_Tracking_ID order by adobe_timestamp) as Next_Actions,
lead(adobe_timestamp) over (partition by Adobe_Tracking_ID order by adobe_timestamp) as Next_Timestamp,
FROM `nbcu-ds-sandbox-a-001.Shunchao_Sandbox.Auto_Binge_Table`  a
where Adobe_Tracking_ID is not null
order by Adobe_Tracking_ID, adobe_timestamp) b),

cte2 as (select 
Adobe_Tracking_ID,
adobe_timestamp,
Vdo_Titles,
extract(day from Auto_Binge_Watch_Time)*24+extract(hour from Auto_Binge_Watch_Time)+extract(minute from Auto_Binge_Watch_Time)/60+extract(second from Auto_Binge_Watch_Time)/3600 as Auto_Binge_Watch_Time_Hours,
extract(day from Up_Next_Watch_Time)*24+extract(hour from Up_Next_Watch_Time)+extract(minute from Up_Next_Watch_Time)/60+extract(second from Up_Next_Watch_Time)/3600 as Up_Next_Watch_Time_Hours,
Auto_Binge_No_Ad_Hours,
Up_Next_No_Ad_Hours,
num_seconds_played_no_ads
from cte)


select
CURRENT_DATE(), --Auto_Binge_table should be updated to current date before running this query
Vdo_Titles,
sum(Auto_Binge_Watch_Time_Hours) as Total_Auto_Binge_w_Ad,
sum(Up_Next_Watch_Time_Hours) as Total_Up_Next_w_Ad,
sum(Total_Watch_Time_w_Ad) as Total_Watch_Time_w_Ad,
round(sum(Up_Next_Watch_Time_Hours)/sum(Total_Watch_Time_w_Ad),2) as Prc_Up_Next_w_Ad,
round(sum(Auto_Binge_Watch_Time_Hours)/sum(Total_Watch_Time_w_Ad),2) as Prc_Auto_Binge_w_Ad,
sum(Auto_Binge_No_Ad_Hours) as Total_Auto_Binge_No_Ad,
sum(Up_Next_No_Ad_Hours) as Total_Up_Next_No_Ad,
sum(num_seconds_played_no_ads) as Total_Watch_hour_no_Ad,
round(sum(Up_Next_No_Ad_Hours)/sum(num_seconds_played_no_ads),2) as Prc_Up_Next_No_Ad,
round(sum(Auto_Binge_No_Ad_Hours)/sum(num_seconds_played_no_ads),2) as Prc_Auto_Binge_No_Ad
from (select 
Vdo_Titles,
Adobe_Tracking_ID,
sum(Auto_Binge_Watch_Time_Hours) as Auto_Binge_Watch_Time_Hours,
sum(Up_Next_Watch_Time_Hours) as Up_Next_Watch_Time_Hours,
sum(Auto_Binge_No_Ad_Hours) as Auto_Binge_No_Ad_Hours,
sum(Up_Next_No_Ad_Hours) as Up_Next_No_Ad_Hours,
extract(day from (max(adobe_timestamp)-min(adobe_timestamp)))*24+extract(hour from (max(adobe_timestamp)-min(adobe_timestamp)))+extract(minute from (max(adobe_timestamp)-min(adobe_timestamp)))/60+extract(second from (max(adobe_timestamp)-min(adobe_timestamp)))/3600 as Total_Watch_Time_w_Ad,
sum(num_seconds_played_no_ads) as num_seconds_played_no_ads
from cte2
group by 1, 2) c
group by 1,2




