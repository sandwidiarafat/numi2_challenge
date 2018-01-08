

select extract(year from last_update_time) as year, 
extract(month from last_update_time) as month, 
extract(day from last_update_time) as day, 
extract(hour from last_update_time) as hour, 
count(*) as cnt 
from food_log 
group by year, month, day, hour
order by year, month, day, hour


select extract(year from last_update_time) as year, 
extract(month from last_update_time) as month, 
extract(day from last_update_time) as day, 
extract(hour from last_update_time) as hour, 
count(*) as cnt 
from activity_log 
group by year, month, day, hour
order by year, month, day, hour

select extract(year from last_update_time) as year, 
extract(month from last_update_time) as month, 
extract(day from last_update_time) as day, 
extract(hour from last_update_time) as hour, 
count(*) as cnt 
from user_profile_history
group by year, month, day, hour
order by year, month, day, hour